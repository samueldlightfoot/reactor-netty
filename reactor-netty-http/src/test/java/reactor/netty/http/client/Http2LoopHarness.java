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

import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

/**
 * Standalone HTTP/2 (H2C) client-loop throughput harness for A/B'ing {@code Http2Pool} changes end-to-end.
 * Drives {@code threads} concurrent blocking request loops against a loopback H2C echo server through a
 * warm, multiplexed pool ({@code maxConn} connections × {@code maxStreams} streams) and reports throughput.
 * No fast-acquire introspection — compiles against any {@code Http2Pool}, so it is used to A/B the
 * loop-side pool micro-opts on branch {@code http2-perf} (patched) vs the reverted {@code Http2Pool}
 * (baseline) with the same binary.
 *
 * <p>Not a test (no {@code @Test}); run via the {@code h2Harness} Gradle task (JDK17-pinned). Profile/throughput
 * A/B: build patched, run; {@code git checkout <base> -- Http2Pool.java}, rebuild, run; compare.
 *
 * <pre>{@code
 * ./gradlew :reactor-netty-http:h2Harness -Pharness.durationSec=30 -Pharness.warmupSec=10
 * }</pre>
 */
public final class Http2LoopHarness {

	public static void main(String[] args) throws Exception {
		int durationSec = intProp("harness.durationSec", 30);
		int warmupSec = intProp("harness.warmupSec", 10);
		int threads = intProp("harness.threads", 64);
		int minConn = intProp("harness.minConn", 4);
		int maxConn = intProp("harness.maxConn", 4);
		int maxStreams = intProp("harness.maxStreams", 50);
		int clientLoops = intProp("harness.clientLoops", 4);
		int serverLoopCount = intProp("harness.serverLoops", 8);
		int bodyBytes = intProp("harness.bodyBytes", 16);

		silenceLogging();

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < bodyBytes; i++) {
			sb.append('x');
		}
		String payload = sb.toString();
		LoopResources serverLoops = LoopResources.create("h2h-srv", serverLoopCount, true);
		LoopResources clientLoopResources = LoopResources.create("h2h-cli", clientLoops, true);

		DisposableServer server = HttpServer.create()
				.protocol(HttpProtocol.H2C)
				.runOn(serverLoops)
				.httpRequestDecoder(spec -> spec.h2cMaxContentLength(256))
				.route(r -> r.get("/", (req, res) -> res.sendString(Mono.just(payload))))
				.bindNow();

		ConnectionProvider provider = ConnectionProvider.builder("h2harness")
				.allocationStrategy(Http2AllocationStrategy.builder()
						.maxConnections(maxConn)
						.minConnections(minConn)
						.maxConcurrentStreams(maxStreams)
						.build())
				.build();

		HttpClient client = HttpClient.create(provider)
				.protocol(HttpProtocol.H2C)
				.runOn(clientLoopResources)
				.host("127.0.0.1")
				.port(server.port())
				.wiretap(false);

		AtomicLong measured = new AtomicLong();
		AtomicLong total = new AtomicLong();
		AtomicLong errors = new AtomicLong();
		volatileHolder.running = true;
		volatileHolder.counting = false;

		List<Thread> drivers = new ArrayList<>();
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
			}, "h2h-driver-" + i);
			t.setDaemon(true);
			drivers.add(t);
			t.start();
		}
		started.await();

		System.out.printf("harness up: threads=%d minConn=%d maxConn=%d maxStreams=%d clientLoops=%d " +
						"serverLoops=%d bodyBytes=%d warmup=%ds window=%ds%n",
				threads, minConn, maxConn, maxStreams, clientLoops, serverLoopCount, bodyBytes, warmupSec, durationSec);

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

		System.out.println("==== Http2LoopHarness ====");
		System.out.printf("minConn=%d maxConn=%d maxStreams=%d threads=%d clientLoops=%d serverLoops=%d bodyBytes=%d%n",
				minConn, maxConn, maxStreams, threads, clientLoops, serverLoopCount, bodyBytes);
		System.out.printf("window=%.1fs  requests(window)=%d  errors=%d%n", seconds, window, errors.get());
		System.out.printf("throughput   = %,.0f req/s%n", throughput);
		System.out.printf("meanLatency  ~ %.1f us  (Little's law: threads / throughput)%n", meanLatencyUs);
		System.out.println("==========================");

		provider.disposeLater().block(Duration.ofSeconds(5));
		server.disposeNow();
		serverLoops.disposeLater().block(Duration.ofSeconds(5));
		clientLoopResources.disposeLater().block(Duration.ofSeconds(5));
	}

	static final class Flags {
		volatile boolean running;
		volatile boolean counting;
	}

	static final Flags volatileHolder = new Flags();

	static int intProp(String name, int def) {
		String v = System.getProperty(name);
		return v == null ? def : Integer.parseInt(v.trim());
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
