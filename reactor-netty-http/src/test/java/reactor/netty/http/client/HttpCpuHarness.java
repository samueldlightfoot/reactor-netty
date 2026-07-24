/*
 * Copyright (c) 2026 VMware, Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.netty.http.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.Http11SslContextSpec;
import reactor.netty.http.Http2SslContextSpec;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.SslProvider;

/**
 * Unified HTTP/1.1-vs-HTTP/2 client-loop CPU harness for attributing the H2-over-H1 per-request CPU delta.
 * ONE driver + ONE measurement path; only {@code -Pharness.protocol} (H1|H2) and {@code -Pharness.tls}
 * differ, so an H1 run and an H2 run are apples-to-apples. Drives {@code threads} concurrent blocking
 * request loops against a loopback echo server and reports throughput; attach async-profiler during the
 * window to get the per-subsystem CPU breakdown.
 *
 * <p>Loop threads are named {@code cpuh-cli-*} (client) and {@code cpuh-srv-*} (server) so a collapsed
 * async-profiler capture can be split by side:
 * {@code asprof -d 60 -e cpu -t -o collapsed -f cpu.txt <pid>} then filter {@code cpuh-cli}/{@code cpuh-srv}.
 *
 * <p>Not a test (no {@code @Test}); run via the {@code cpuHarness} Gradle task (JDK17-pinned). Example:
 * <pre>{@code
 * ./gradlew :reactor-netty-http:cpuHarness -Pharness.protocol=H2 -Pharness.tls=true -Pharness.durationSec=60
 * }</pre>
 */
public final class HttpCpuHarness {

	public static void main(String[] args) throws Exception {
		int durationSec = intProp("harness.durationSec", 60);
		int warmupSec = intProp("harness.warmupSec", 15);
		int threads = intProp("harness.threads", 64);
		int clientLoops = intProp("harness.clientLoops", 4);
		int serverLoopCount = intProp("harness.serverLoops", 8);
		int bodyBytes = intProp("harness.bodyBytes", 16);
		String protocol = System.getProperty("harness.protocol", "H2").trim().toUpperCase();
		boolean tls = boolProp("harness.tls", true);
		String mode = System.getProperty("harness.mode", "block").trim().toLowerCase();
		boolean async = mode.equals("async") || mode.equals("subscribe");
		boolean h2 = protocol.startsWith("H2");
		// H2: natural multiplexed shape (few conns × many streams). H1: one warm conn per thread (no
		// pool starvation at this thread count), so throughput reflects CPU/req, not acquire waiting.
		int maxStreams = intProp("harness.maxStreams", 25);
		int h2MaxConn = intProp("harness.maxConn", 4);
		int h2MinConn = intProp("harness.minConn", h2MaxConn);
		int h1MaxConn = intProp("harness.maxConn", threads);

		silenceLogging();

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < bodyBytes; i++) {
			sb.append('x');
		}
		String payload = sb.toString();

		X509Bundle ssc = new CertificateBuilder().subject("CN=localhost").setIsCertificateAuthority(true).buildSelfSigned();
		SslProvider.GenericSslContextSpec<?> serverCtx = h2
				? Http2SslContextSpec.forServer(ssc.toTempCertChainPem(), ssc.toTempPrivateKeyPem())
				: Http11SslContextSpec.forServer(ssc.toTempCertChainPem(), ssc.toTempPrivateKeyPem());
		SslProvider.GenericSslContextSpec<?> clientCtx = h2
				? Http2SslContextSpec.forClient().configure(b -> b.trustManager(InsecureTrustManagerFactory.INSTANCE))
				: Http11SslContextSpec.forClient().configure(b -> b.trustManager(InsecureTrustManagerFactory.INSTANCE));

		HttpProtocol proto = h2 ? (tls ? HttpProtocol.H2 : HttpProtocol.H2C) : HttpProtocol.HTTP11;

		LoopResources serverLoops = LoopResources.create("cpuh-srv", serverLoopCount, true);
		LoopResources clientLoopResources = LoopResources.create("cpuh-cli", clientLoops, true);

		HttpServer serverBuilder = HttpServer.create()
				.protocol(proto)
				.runOn(serverLoops)
				.route(r -> r.get("/", (req, res) -> res.sendString(Mono.just(payload))));
		if (tls) {
			serverBuilder = serverBuilder.secure(spec -> spec.sslContext(serverCtx));
		}
		DisposableServer server = serverBuilder.bindNow();

		ConnectionProvider provider = h2
				? ConnectionProvider.builder("cpuh2")
						.allocationStrategy(Http2AllocationStrategy.builder()
								.maxConnections(h2MaxConn)
								.minConnections(h2MinConn)
								.maxConcurrentStreams(maxStreams)
								.build())
						.build()
				: ConnectionProvider.builder("cpuh1")
						.maxConnections(h1MaxConn)
						.pendingAcquireMaxCount(4 * h1MaxConn)
						.build();

		HttpClient clientBuilder = HttpClient.create(provider)
				.protocol(proto)
				.runOn(clientLoopResources)
				.host("127.0.0.1")
				.port(server.port())
				.wiretap(false);
		if (tls) {
			clientBuilder = clientBuilder.secure(spec -> spec.sslContext(clientCtx));
		}
		HttpClient client = clientBuilder;

		AtomicLong measured = new AtomicLong();
		AtomicLong total = new AtomicLong();
		AtomicLong errors = new AtomicLong();
		volatileHolder.running = true;
		volatileHolder.counting = false;

		List<Thread> drivers = new ArrayList<>();
		if (async) {
			// Non-blocking driver: hold a fixed in-flight window of `threads` requests, each response
			// re-subscribing the next one from its completion callback — which runs on the client loop.
			// No request ever parks a driver thread, so the loop never pays the per-request unpark it
			// pays under .block() (a load-generator artifact, not client work). Same offered concurrency
			// as the blocking driver, so the two modes are apples-to-apples.
			Mono<Integer> oneReq = client.get().uri("/").responseContent().aggregate()
					.map(buf -> {
						int n = buf.readableBytes();
						buf.release();
						return n;
					});
			AsyncDriver driver = new AsyncDriver(oneReq, total, measured, errors, volatileHolder);
			for (int i = 0; i < threads; i++) {
				driver.fire();
			}
		}
		else {
			CountDownLatch started = new CountDownLatch(threads);
			for (int i = 0; i < threads; i++) {
				Thread t = new Thread(() -> {
					started.countDown();
					while (volatileHolder.running) {
						try {
							client.get().uri("/").responseContent().aggregate().block(Duration.ofSeconds(5));
							total.incrementAndGet();
							if (volatileHolder.counting) {
								measured.incrementAndGet();
							}
						}
						catch (Throwable ex) {
							errors.incrementAndGet();
						}
					}
				}, "cpuh-driver-" + i);
				t.setDaemon(true);
				drivers.add(t);
				t.start();
			}
			started.await();
		}

		int reportConn = h2 ? h2MaxConn : h1MaxConn;
		System.out.printf("harness up: protocol=%s tls=%b mode=%s threads=%d conn=%d maxStreams=%s clientLoops=%d " +
						"serverLoops=%d bodyBytes=%d warmup=%ds window=%ds — attach async-profiler now " +
						"(cpuh-cli-* / cpuh-srv-* loop threads)%n",
				proto, tls, async ? "async" : "block", threads, reportConn, h2 ? String.valueOf(maxStreams) : "n/a",
				clientLoops, serverLoopCount, bodyBytes, warmupSec, durationSec);

		Thread.sleep(warmupSec * 1000L);
		volatileHolder.counting = true;
		long startNanos = System.nanoTime();
		Thread.sleep(durationSec * 1000L);
		long elapsedNanos = System.nanoTime() - startNanos;
		volatileHolder.counting = false;
		volatileHolder.running = false;
		for (Thread t : drivers) {
			t.join(Duration.ofSeconds(10).toMillis());
		}

		double seconds = elapsedNanos / 1_000_000_000.0;
		long window = measured.get();
		double throughput = window / seconds;
		double meanLatencyUs = throughput > 0 ? (threads / throughput) * 1_000_000.0 : Double.NaN;

		System.out.println("==== HttpCpuHarness ====");
		System.out.printf("protocol=%s tls=%b mode=%s conn=%d maxStreams=%s threads=%d clientLoops=%d serverLoops=%d bodyBytes=%d%n",
				proto, tls, async ? "async" : "block", reportConn, h2 ? String.valueOf(maxStreams) : "n/a", threads, clientLoops, serverLoopCount, bodyBytes);
		System.out.printf("window=%.1fs  requests(window)=%d  errors=%d%n", seconds, window, errors.get());
		System.out.printf("throughput   = %,.0f req/s%n", throughput);
		System.out.printf("meanLatency  ~ %.1f us  (Little's law: threads / throughput)%n", meanLatencyUs);
		System.out.println("========================");

		provider.disposeLater().block(Duration.ofSeconds(5));
		server.disposeNow();
		serverLoops.disposeLater().block(Duration.ofSeconds(5));
		clientLoopResources.disposeLater().block(Duration.ofSeconds(5));
	}

	static final class Flags {
		volatile boolean running;
		volatile boolean counting;
	}

	// One non-blocking in-flight slot: subscribe, and on the terminal signal re-subscribe the next
	// request from the callback (which fires on the client loop), keeping the in-flight window constant
	// without a driver thread ever parking. `subscribe()` returns before the async response arrives, so
	// re-firing from the callback does not grow the stack.
	static final class AsyncDriver {
		final Mono<Integer> req;
		final AtomicLong total;
		final AtomicLong measured;
		final AtomicLong errors;
		final Flags flags;

		AsyncDriver(Mono<Integer> req, AtomicLong total, AtomicLong measured, AtomicLong errors, Flags flags) {
			this.req = req;
			this.total = total;
			this.measured = measured;
			this.errors = errors;
			this.flags = flags;
		}

		void fire() {
			req.subscribe(n -> {
				total.incrementAndGet();
				if (flags.counting) {
					measured.incrementAndGet();
				}
				if (flags.running) {
					fire();
				}
			}, ex -> {
				errors.incrementAndGet();
				if (flags.running) {
					fire();
				}
			});
		}
	}

	static final Flags volatileHolder = new Flags();

	static int intProp(String name, int def) {
		String v = System.getProperty(name);
		return v == null ? def : Integer.parseInt(v.trim());
	}

	static boolean boolProp(String name, boolean def) {
		String v = System.getProperty(name);
		return v == null ? def : Boolean.parseBoolean(v.trim());
	}

	// DEBUG logging dominates loopback throughput; pin every configured logger to WARN for the
	// measurement (the test logback config sets reactor.netty to DEBUG, so root alone is not enough).
	static void silenceLogging() {
		try {
			ch.qos.logback.classic.LoggerContext ctx =
					(ch.qos.logback.classic.LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory();
			for (ch.qos.logback.classic.Logger l : ctx.getLoggerList()) {
				l.setLevel(ch.qos.logback.classic.Level.WARN);
			}
		}
		catch (Throwable ignored) {
			// logback not on the classpath; leave logging as configured
		}
	}
}
