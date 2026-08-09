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
package reactor.netty.http.server;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Sizes a hypothetical single-entry memo of {@link ConnectionInfo} keyed on the raw {@code Host}
 * header, against the current per-request parse.
 *
 * <p>The arms that matter are {@link #todayTypical} (what runs now), {@link #memoHitTypical} (the
 * best case a memo could reach) and {@link #headerLookupOnlyTypical} — the prove-a-negative arm.
 * {@code headerLookupOnly} is the floor no memo can go below, because the guard needs the header
 * value in hand to compare against the cached key. If it lands close to {@code today}, the memo has
 * nothing to save and the design is dead regardless of how correct it is.
 *
 * <p>Every cached key is a defensive copy, so {@link String#equals} cannot take its {@code this ==
 * anObject} shortcut. Netty hands back the same {@code String} instance here because the header was
 * set from one; a real decoder builds a fresh {@code String} per request, and a guard measured on
 * the identity fast path would be fiction.
 */
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
@Fork(6)
@State(Scope.Benchmark)
public class ConnectionInfoMemoBenchmark {

	static final String SHORT_HOST = "localhost:8080";
	static final String TYPICAL_HOST = "api.example.com:8443";
	static final String LONG_HOST = "service-a.eu-west-1.internal.example.com:8443";

	final InetSocketAddress localAddress = new InetSocketAddress("127.0.0.1", 8080);
	final InetSocketAddress remoteAddress = new InetSocketAddress("127.0.0.1", 54321);

	HttpRequest shortRequest;
	HttpRequest typicalRequest;
	HttpRequest longRequest;
	HttpRequest otherTypicalRequest;

	String cachedShortKey;
	String cachedTypicalKey;
	String cachedLongKey;
	ConnectionInfo cachedInfo;

	boolean alternate;

	@Setup(Level.Trial)
	public void setup() {
		shortRequest = request(SHORT_HOST);
		typicalRequest = request(TYPICAL_HOST);
		longRequest = request(LONG_HOST);
		otherTypicalRequest = request("api.other-vhost.com:8443");

		cachedShortKey = copyOf(SHORT_HOST);
		cachedTypicalKey = copyOf(TYPICAL_HOST);
		cachedLongKey = copyOf(LONG_HOST);
		cachedInfo = ConnectionInfo.from(typicalRequest, false, localAddress, remoteAddress, null);
	}

	static HttpRequest request(String host) {
		HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
		request.headers().set(HttpHeaderNames.HOST, host);
		return request;
	}

	static String copyOf(String s) {
		return new String(s.toCharArray());
	}

	// --- arm A: what runs today -------------------------------------------------------------

	@Benchmark
	public ConnectionInfo todayShort() {
		return ConnectionInfo.from(shortRequest, false, localAddress, remoteAddress, null);
	}

	@Benchmark
	public ConnectionInfo todayTypical() {
		return ConnectionInfo.from(typicalRequest, false, localAddress, remoteAddress, null);
	}

	@Benchmark
	public ConnectionInfo todayLong() {
		return ConnectionInfo.from(longRequest, false, localAddress, remoteAddress, null);
	}

	// --- arm E: prove-a-negative, the floor a memo cannot go below --------------------------

	@Benchmark
	public String headerLookupOnlyShort() {
		return shortRequest.headers().get(HttpHeaderNames.HOST);
	}

	@Benchmark
	public String headerLookupOnlyTypical() {
		return typicalRequest.headers().get(HttpHeaderNames.HOST);
	}

	@Benchmark
	public String headerLookupOnlyLong() {
		return longRequest.headers().get(HttpHeaderNames.HOST);
	}

	// --- arm B: memo hit ---------------------------------------------------------------------

	@Benchmark
	public ConnectionInfo memoHitShort() {
		String header = shortRequest.headers().get(HttpHeaderNames.HOST);
		if (cachedShortKey.equals(header)) {
			return cachedInfo;
		}
		return ConnectionInfo.from(shortRequest, false, localAddress, remoteAddress, null);
	}

	@Benchmark
	public ConnectionInfo memoHitTypical() {
		String header = typicalRequest.headers().get(HttpHeaderNames.HOST);
		if (cachedTypicalKey.equals(header)) {
			return cachedInfo;
		}
		return ConnectionInfo.from(typicalRequest, false, localAddress, remoteAddress, null);
	}

	@Benchmark
	public ConnectionInfo memoHitLong() {
		String header = longRequest.headers().get(HttpHeaderNames.HOST);
		if (cachedLongKey.equals(header)) {
			return cachedInfo;
		}
		return ConnectionInfo.from(longRequest, false, localAddress, remoteAddress, null);
	}

	// --- arm C: memo miss, must land at or above arm A --------------------------------------

	@Benchmark
	public ConnectionInfo memoMissTypical() {
		HttpRequest request = (alternate = !alternate) ? typicalRequest : otherTypicalRequest;
		String header = request.headers().get(HttpHeaderNames.HOST);
		if (cachedTypicalKey.equals(header)) {
			return cachedInfo;
		}
		cachedTypicalKey = header;
		return cachedInfo = ConnectionInfo.from(request, false, localAddress, remoteAddress, null);
	}

	// --- arm D: the guard on its own ---------------------------------------------------------

	@Benchmark
	public boolean guardOnlyShort() {
		return cachedShortKey.equals(SHORT_HOST);
	}

	@Benchmark
	public boolean guardOnlyTypical() {
		return cachedTypicalKey.equals(TYPICAL_HOST);
	}

	@Benchmark
	public boolean guardOnlyLong() {
		return cachedLongKey.equals(LONG_HOST);
	}
}
