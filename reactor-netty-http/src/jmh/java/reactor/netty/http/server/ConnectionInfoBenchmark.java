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
 * Microbenchmark for {@link ConnectionInfo#from} Host-header parsing.
 *
 * <p>{@link #hostWithPort} exercises the port-parse path targeted by the "avoid throwaway substring"
 * change — run with {@code -prof gc}, the {@code gc.alloc.rate.norm} (bytes/op) delta between the
 * baseline ({@code Integer.parseInt(header.substring(...))}) and patched arms isolates the removed
 * substring. {@link #hostNoPort} is a control: it never enters the {@code :}-port branch, so its
 * bytes/op must stay identical across arms — a drift gate on the harness itself.
 */
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Benchmark)
public class ConnectionInfoBenchmark {

	final InetSocketAddress localAddress = new InetSocketAddress("127.0.0.1", 8080);
	final InetSocketAddress remoteAddress = new InetSocketAddress("127.0.0.1", 54321);

	HttpRequest requestWithPort;
	HttpRequest requestNoPort;

	@Setup(Level.Trial)
	public void setup() {
		requestWithPort = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
		requestWithPort.headers().set(HttpHeaderNames.HOST, "localhost:8080");
		requestNoPort = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
		requestNoPort.headers().set(HttpHeaderNames.HOST, "localhost");
	}

	@Benchmark
	public ConnectionInfo hostWithPort() {
		return ConnectionInfo.from(requestWithPort, false, localAddress, remoteAddress, null);
	}

	@Benchmark
	public ConnectionInfo hostNoPort() {
		return ConnectionInfo.from(requestNoPort, false, localAddress, remoteAddress, null);
	}
}
