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
import java.util.concurrent.atomic.AtomicLong;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.X509Bundle;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.http.server.reactive.HttpHandler;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.config.EnableWebFlux;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.adapter.WebHttpHandlerBuilder;
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
 * Companion to {@link HttpCpuHarness}: the same client-loop CPU harness shape, but driving a genuine
 * Spring WebFlux {@code WebClient} against a genuine Spring WebFlux {@code @RestController} server
 * (real {@code DispatcherHandler}/{@code RequestMappingHandlerMapping} dispatch via
 * {@link WebHttpHandlerBuilder} + {@link ReactorHttpHandlerAdapter}) instead of raw
 * {@code reactor.netty.http.client.HttpClient}/{@code HttpServer}. Answers "what does a full WebClient
 * request cycle cost, not just the client loop" — {@code HttpCpuHarness}'s {@code -Pharness.uriMode}
 * only approximated WebFlux's request shape; this runs the real classes.
 *
 * <p>No {@code harness.mode}/{@code harness.uriMode} knobs here: a {@code WebClient} chain is always
 * non-blocking (a real WebFlux handler never calls {@code .block()}), and it always resolves a genuine
 * absolute URI, so there is no block/async or relative/absolute branch to select.
 *
 * <p>Loop threads are named {@code cpuh-cli-*} (client) and {@code cpuh-srv-*} (server), matching
 * {@code HttpCpuHarness}, so the same collapsed-profile filtering recipe applies:
 * {@code asprof -d 60 -e cpu -t -o collapsed -f cpu.txt <pid>} then filter {@code cpuh-cli}/{@code cpuh-srv}.
 *
 * <p>Not a test; run via the {@code webClientHarness} Gradle task (JDK17-pinned, its own
 * {@code webclientHarness} source set). Example:
 * <pre>{@code
 * ./gradlew :reactor-netty-http:webClientHarness -Pharness.protocol=H2 -Pharness.tls=true -Pharness.durationSec=60
 * }</pre>
 */
public final class WebClientCpuHarness {

	static volatile String payload;

	public static void main(String[] args) throws Exception {
		int durationSec = intProp("harness.durationSec", 60);
		int warmupSec = intProp("harness.warmupSec", 15);
		int threads = intProp("harness.threads", 64);
		int clientLoops = intProp("harness.clientLoops", 4);
		int serverLoopCount = intProp("harness.serverLoops", 8);
		int bodyBytes = intProp("harness.bodyBytes", 16);
		String protocol = System.getProperty("harness.protocol", "H2").trim().toUpperCase();
		boolean tls = boolProp("harness.tls", true);
		// tasks/select-media-type-percall/client-efficiency-guide.md levers 1+2: declaring `produces`
		// on the handler lets selectMediaType copy the cached producible-types list instead of walking
		// every HttpMessageWriter (measured ~13x cheaper); a concrete client Accept keeps the
		// negotiation's compatible-type list at length 1. Same harness, one flag, so an A/B against the
		// unoptimized default is apples-to-apples.
		boolean mediaTypeOptimized = boolProp("harness.mediaTypeOptimized", false);
		boolean h2 = protocol.startsWith("H2");
		int maxStreams = intProp("harness.maxStreams", 25);
		int h2MaxConn = intProp("harness.maxConn", 4);
		int h2MinConn = intProp("harness.minConn", h2MaxConn);
		int h1MaxConn = intProp("harness.maxConn", threads);

		silenceLogging();

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < bodyBytes; i++) {
			sb.append('x');
		}
		payload = sb.toString();

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

		// Real WebFlux dispatch: @EnableWebFlux wires DispatcherHandler + RequestMappingHandlerMapping/
		// Adapter exactly as Spring Boot's autoconfiguration would, sourced entirely from spring-context/
		// spring-webflux/spring-web on the classpath — no spring-boot-starter-webflux dependency needed.
		Class<?> serverConfigClass = mediaTypeOptimized ? ServerConfigOptimized.class : ServerConfig.class;
		AnnotationConfigApplicationContext appCtx = new AnnotationConfigApplicationContext(serverConfigClass);
		HttpHandler httpHandler = WebHttpHandlerBuilder.applicationContext(appCtx).build();
		ReactorHttpHandlerAdapter adapter = new ReactorHttpHandlerAdapter(httpHandler);

		HttpServer serverBuilder = HttpServer.create()
				.protocol(proto)
				.runOn(serverLoops)
				.handle(adapter);
		if (tls) {
			serverBuilder = serverBuilder.secure(spec -> spec.sslContext(serverCtx));
		}
		DisposableServer server = serverBuilder.bindNow();

		ConnectionProvider provider = h2
				? ConnectionProvider.builder("wch2")
						.allocationStrategy(Http2AllocationStrategy.builder()
								.maxConnections(h2MaxConn)
								.minConnections(h2MinConn)
								.maxConcurrentStreams(maxStreams)
								.build())
						.build()
				: ConnectionProvider.builder("wch1")
						.maxConnections(h1MaxConn)
						.pendingAcquireMaxCount(4 * h1MaxConn)
						.build();

		HttpClient nettyClientBuilder = HttpClient.create(provider)
				.protocol(proto)
				.runOn(clientLoopResources)
				.host("127.0.0.1")
				.port(server.port())
				.wiretap(false);
		if (tls) {
			nettyClientBuilder = nettyClientBuilder.secure(spec -> spec.sslContext(clientCtx));
		}
		HttpClient nettyClient = nettyClientBuilder;

		String baseUrl = (tls ? "https" : "http") + "://127.0.0.1:" + server.port();
		WebClient webClient = WebClient.builder()
				.clientConnector(new ReactorClientHttpConnector(nettyClient))
				.baseUrl(baseUrl)
				.build();

		AtomicLong measured = new AtomicLong();
		AtomicLong total = new AtomicLong();
		AtomicLong errors = new AtomicLong();
		volatileHolder.running = true;
		volatileHolder.counting = false;

		// Fixed in-flight window of `threads` requests, each re-subscribing the next one from its
		// completion callback (runs on the client loop) — same shape as HttpCpuHarness's AsyncDriver,
		// ported onto WebClient's Mono chain. No request ever parks a driver thread.
		WebClient.RequestHeadersSpec<?> requestSpec = webClient.get().uri("/");
		if (mediaTypeOptimized) {
			requestSpec = requestSpec.accept(MediaType.TEXT_PLAIN);
		}
		Mono<Integer> oneReq = requestSpec.retrieve().bodyToMono(String.class).map(String::length);
		AsyncDriver driver = new AsyncDriver(oneReq, total, measured, errors, volatileHolder);
		for (int i = 0; i < threads; i++) {
			driver.fire();
		}

		int reportConn = h2 ? h2MaxConn : h1MaxConn;
		System.out.printf("harness up: protocol=%s tls=%b mediaTypeOptimized=%b threads=%d conn=%d maxStreams=%s clientLoops=%d " +
						"serverLoops=%d bodyBytes=%d warmup=%ds window=%ds — attach async-profiler now " +
						"(cpuh-cli-* / cpuh-srv-* loop threads)%n",
				proto, tls, mediaTypeOptimized, threads, reportConn, h2 ? String.valueOf(maxStreams) : "n/a",
				clientLoops, serverLoopCount, bodyBytes, warmupSec, durationSec);

		Thread.sleep(warmupSec * 1000L);
		volatileHolder.counting = true;
		long startNanos = System.nanoTime();
		Thread.sleep(durationSec * 1000L);
		long elapsedNanos = System.nanoTime() - startNanos;
		volatileHolder.counting = false;
		volatileHolder.running = false;

		double seconds = elapsedNanos / 1_000_000_000.0;
		long window = measured.get();
		double throughput = window / seconds;
		double meanLatencyUs = throughput > 0 ? (threads / throughput) * 1_000_000.0 : Double.NaN;

		System.out.println("==== WebClientCpuHarness ====");
		System.out.printf("protocol=%s tls=%b mediaTypeOptimized=%b conn=%d maxStreams=%s threads=%d clientLoops=%d serverLoops=%d bodyBytes=%d%n",
				proto, tls, mediaTypeOptimized, reportConn, h2 ? String.valueOf(maxStreams) : "n/a", threads, clientLoops, serverLoopCount, bodyBytes);
		System.out.printf("window=%.1fs  requests(window)=%d  errors=%d%n", seconds, window, errors.get());
		System.out.printf("throughput   = %,.0f req/s%n", throughput);
		System.out.printf("meanLatency  ~ %.1f us  (Little's law: threads / throughput)%n", meanLatencyUs);
		System.out.println("==============================");

		provider.disposeLater().block(Duration.ofSeconds(5));
		server.disposeNow();
		appCtx.close();
		serverLoops.disposeLater().block(Duration.ofSeconds(5));
		clientLoopResources.disposeLater().block(Duration.ofSeconds(5));
	}

	@Configuration
	@EnableWebFlux
	static class ServerConfig {
		@Bean
		RestEndpoint restEndpoint() {
			return new RestEndpoint();
		}
	}

	@RestController
	static final class RestEndpoint {
		@GetMapping("/")
		Mono<String> get() {
			return Mono.just(payload);
		}
	}

	// mediaTypeOptimized=true arm: declaring `produces` populates HandlerMapping.PRODUCIBLE_MEDIA_TYPES_ATTRIBUTE
	// at mapping time, so HandlerResultHandlerSupport.selectMediaType copies the cached list instead of
	// walking every HttpMessageWriter's canWrite/getWritableMediaTypes on every response.
	@Configuration
	@EnableWebFlux
	static class ServerConfigOptimized {
		@Bean
		RestEndpointOptimized restEndpoint() {
			return new RestEndpointOptimized();
		}
	}

	@RestController
	static final class RestEndpointOptimized {
		@GetMapping(value = "/", produces = MediaType.TEXT_PLAIN_VALUE)
		Mono<String> get() {
			return Mono.just(payload);
		}
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

	// This sourceSet has no logback config of its own, so logback falls back to BasicConfigurator
	// (root at DEBUG) on first touch. Call this before any server/client construction logs anything:
	// setting root to WARN here is enough — loggers created later inherit it unless overridden.
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
