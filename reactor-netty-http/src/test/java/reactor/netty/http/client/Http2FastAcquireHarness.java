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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

/**
 * Standalone A/B load harness for the HTTP/2 pool fast-acquire path over a lighter reactor-netty
 * {@link HttpClient} fan-out (no Spring). It drives {@code threads} concurrent blocking request loops
 * (one acquire in flight per thread) against a loopback H2C echo server through a warm, multiplexed
 * pool, and reports throughput plus the fast-acquire engagement ratio.
 *
 * <p>Not a test (no {@code @Test}); run via the {@code h2Harness} Gradle task. A/B is the same binary
 * twice, toggling {@code -Dreactor.netty.pool.h2.fastAcquire}. See {@code tasks/http2-pool-perf}.
 *
 * <pre>{@code
 * ./gradlew :reactor-netty-http:h2Harness -Ph2FastAcquire=false -Pharness.durationSec=20
 * ./gradlew :reactor-netty-http:h2Harness -Ph2FastAcquire=true  -Pharness.durationSec=20
 * }</pre>
 */
public final class Http2FastAcquireHarness {

	public static void main(String[] args) throws Exception {
		int durationSec = intProp("harness.durationSec", 20);
		int warmupSec = intProp("harness.warmupSec", 5);
		int threads = intProp("harness.threads", 64);
		int minConn = intProp("harness.minConn", 4);
		int maxConn = intProp("harness.maxConn", 4);
		int maxStreams = intProp("harness.maxStreams", 50);
		int clientLoops = intProp("harness.clientLoops", Runtime.getRuntime().availableProcessors());
		int serverLoopCount = intProp("harness.serverLoops", 4);
		int bodyBytes = intProp("harness.bodyBytes", 16);
		boolean fastAcquire = Boolean.getBoolean("reactor.netty.pool.h2.fastAcquire");

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

		AtomicLong measured = new AtomicLong();   // completions in the measurement window (throughput)
		AtomicLong total = new AtomicLong();       // all completions incl. warm-up (engagement denominator)
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

		long fastDelivered = sumFastAcquireDelivered(findHttp2Pools(client, provider));
		double seconds = elapsedNanos / 1_000_000_000.0;
		long window = measured.get();
		double throughput = window / seconds;
		double meanLatencyUs = throughput > 0 ? (threads / throughput) * 1_000_000.0 : Double.NaN;
		double engagement = total.get() > 0 ? (100.0 * fastDelivered / total.get()) : Double.NaN;

		System.out.println("==== Http2FastAcquireHarness ====");
		System.out.printf("flag fastAcquire=%s  minConn=%d maxConn=%d maxStreams=%d threads=%d clientLoops=%d%n",
				fastAcquire, minConn, maxConn, maxStreams, threads, clientLoops);
		System.out.printf("window=%.1fs  requests(window)=%d  errors=%d%n", seconds, window, errors.get());
		System.out.printf("throughput   = %,.0f req/s%n", throughput);
		System.out.printf("meanLatency  ~ %.1f us  (Little's law: threads / throughput)%n", meanLatencyUs);
		System.out.printf("fastDelivered= %d (cumulative)  totalRequests=%d%n", fastDelivered, total.get());
		System.out.printf("engagement   = %.1f%%  (fastDelivered / totalRequests)%n", engagement);
		System.out.println("=================================");

		provider.disposeLater().block(Duration.ofSeconds(5));
		server.disposeNow();
		serverLoops.disposeLater().block(Duration.ofSeconds(5));
		clientLoopResources.disposeLater().block(Duration.ofSeconds(5));
	}

	// A tiny mutable holder for the driver flags (avoids capturing non-final locals).
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

	static long sumFastAcquireDelivered(List<Http2Pool> pools) {
		long sum = 0;
		for (Http2Pool pool : pools) {
			sum += pool.fastAcquireDelivered;
		}
		return sum;
	}

	/**
	 * Bounded reflective search over the reactor object graph for live {@link Http2Pool} instances.
	 * The pool is created lazily inside an {@code Http2ConnectionProvider} wrapped by other layers, so
	 * there is no clean typed handle; this is diagnostic-only code, not a public accessor.
	 */
	static List<Http2Pool> findHttp2Pools(Object... roots) {
		List<Http2Pool> found = new ArrayList<>();
		Set<Object> visited = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
		Deque<Object> queue = new ArrayDeque<>();
		for (Object root : roots) {
			if (root != null) {
				queue.add(root);
			}
		}
		int budget = 200_000;
		while (!queue.isEmpty() && budget-- > 0) {
			Object o = queue.poll();
			if (o == null || !visited.add(o)) {
				continue;
			}
			if (o instanceof Http2Pool) {
				found.add((Http2Pool) o);
				continue;
			}
			Class<?> c = o.getClass();
			if (o instanceof Map) {
				for (Object v : ((Map<?, ?>) o).values()) {
					enqueue(queue, v);
				}
				continue;
			}
			if (o instanceof Iterable) {
				for (Object v : (Iterable<?>) o) {
					enqueue(queue, v);
				}
				continue;
			}
			if (c.isArray()) {
				if (!c.getComponentType().isPrimitive()) {
					int len = Array.getLength(o);
					for (int i = 0; i < len; i++) {
						enqueue(queue, Array.get(o, i));
					}
				}
				continue;
			}
			// Unwrap the common JDK holders the provider chain uses (the H2 provider hangs off an
			// AtomicReference), so the reactor-only field filter below does not dead-end on them.
			if (o instanceof java.util.concurrent.atomic.AtomicReference) {
				enqueue(queue, ((java.util.concurrent.atomic.AtomicReference<?>) o).get());
				continue;
			}
			if (o instanceof java.util.Optional) {
				enqueue(queue, ((java.util.Optional<?>) o).orElse(null));
				continue;
			}
			// Only recurse into reactor-owned objects to keep the graph bounded and avoid JDK
			// reflection-access restrictions.
			if (!c.getName().startsWith("reactor.")) {
				continue;
			}
			for (Class<?> k = c; k != null && k != Object.class; k = k.getSuperclass()) {
				for (Field f : k.getDeclaredFields()) {
					if (Modifier.isStatic(f.getModifiers()) || f.getType().isPrimitive()) {
						continue;
					}
					try {
						f.setAccessible(true);
						enqueue(queue, f.get(o));
					}
					catch (Throwable ignored) {
						// inaccessible field; skip
					}
				}
			}
		}
		return found;
	}

	static void enqueue(Deque<Object> queue, Object v) {
		if (v != null) {
			queue.add(v);
		}
	}

	private Http2FastAcquireHarness() {
	}
}
