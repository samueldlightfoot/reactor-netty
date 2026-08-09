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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.HttpConversionUtil;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;

/**
 * Three probes, one question: can a memo of {@link ConnectionInfo} be keyed on the connection?
 *
 * <p>Probe 1 sends three requests with three different {@code Host} headers down one HTTP/1.1
 * keep-alive socket, on default config with forwarded handling off, and prints what
 * {@code hostName()}/{@code hostPort()} report. Probe 2 replays the same three header values through
 * two memo prototypes — a per-connection cache and a {@code Host}-keyed memo — so the refuted design
 * gets a chance to fail rather than only the sound one getting a chance to pass. Probe 3 checks
 * whether HTTP/2's per-stream {@code :authority} lands on {@code Host}, which is what decides if H2
 * inherits the same hazard.
 */
public final class HostVariesPerRequestProbe {

	static final String[] HOSTS = {"alpha.example:8080", "beta.example:9090", "gamma.example"};

	public static void main(String[] args) throws Exception {
		probeOneKeepAliveSocket();
		probeMemoPrototypes();
		probeHttp2AuthorityMapping();
	}

	// --- probe 1: on the wire, one socket, three Host headers -------------------------------

	static void probeOneKeepAliveSocket() throws Exception {
		DisposableServer server =
				HttpServer.create()
				          .host("127.0.0.1")
				          .port(0)
				          .handle((req, res) -> res.sendString(
				                  Mono.just(req.hostName() + '|' + req.hostPort())))
				          .bindNow();

		System.out.println("=== probe 1: HTTP/1.1 keep-alive, ONE socket, three Host headers ===");
		System.out.printf("%-22s %-10s %-22s %s%n", "Host sent", "port", "hostName()", "hostPort()");

		try (Socket socket = new Socket("127.0.0.1", server.port())) {
			OutputStream out = socket.getOutputStream();
			BufferedReader in = new BufferedReader(
					new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));

			for (int i = 0; i < HOSTS.length; i++) {
				boolean last = i == HOSTS.length - 1;
				out.write(("GET /probe HTTP/1.1\r\nHost: " + HOSTS[i] + "\r\n"
						+ (last ? "Connection: close\r\n" : "") + "\r\n").getBytes(StandardCharsets.US_ASCII));
				out.flush();

				String line;
				int contentLength = -1;
				while ((line = in.readLine()) != null && !line.isEmpty()) {
					if (line.toLowerCase().startsWith("content-length:")) {
						contentLength = Integer.parseInt(line.substring(15).trim());
					}
				}
				char[] body = new char[contentLength];
				int read = 0;
				while (read < contentLength) {
					read += in.read(body, read, contentLength - read);
				}
				String[] parts = new String(body).split("\\|");
				System.out.printf("%-22s %-10s %-22s %s%n", HOSTS[i], "", parts[0], parts[1]);
			}
		}
		System.out.println("socket count: 1 (all three requests above shared it)\n");
		server.disposeNow();
	}

	// --- probe 2: give the refuted design a chance to fail -----------------------------------

	static void probeMemoPrototypes() {
		SocketAddress local = new InetSocketAddress("127.0.0.1", 8080);
		SocketAddress remote = new InetSocketAddress("127.0.0.1", 54321);

		List<HttpRequest> requests = new ArrayList<>();
		for (String host : HOSTS) {
			HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/probe");
			request.headers().set(HttpHeaderNames.HOST, host);
			requests.add(request);
		}

		System.out.println("=== probe 2: two memo prototypes replayed over the same three requests ===");
		System.out.printf("%-22s | %-28s | %s%n", "Host sent", "per-CONNECTION cache", "Host-keyed memo");

		ConnectionInfo connectionCache = null;
		String memoKey = null;
		ConnectionInfo memoValue = null;
		int connectionCacheWrong = 0;
		int memoWrong = 0;

		for (int i = 0; i < requests.size(); i++) {
			HttpRequest request = requests.get(i);
			ConnectionInfo truth = ConnectionInfo.from(request, false, local, remote, null);

			// the refuted design: resolve once per connection, reuse blindly
			if (connectionCache == null) {
				connectionCache = ConnectionInfo.from(request, false, local, remote, null);
			}

			// the sound design: single-entry memo re-validated against the raw Host header
			String header = request.headers().get(HttpHeaderNames.HOST);
			if (memoValue == null || !memoKey.equals(header)) {
				memoKey = header;
				memoValue = ConnectionInfo.from(request, false, local, remote, null);
			}

			if (!describe(connectionCache).equals(describe(truth))) {
				connectionCacheWrong++;
			}
			if (!describe(memoValue).equals(describe(truth))) {
				memoWrong++;
			}
			System.out.printf("%-22s | %-28s | %s%n", HOSTS[i], describe(connectionCache), describe(memoValue));
		}

		System.out.println("wrong answers: per-connection cache = " + connectionCacheWrong
				+ ", Host-keyed memo = " + memoWrong);
		System.out.println((connectionCacheWrong > 0 && memoWrong == 0)
				? "PROBE VALID: the refuted design fails here, the sound one does not.\n"
				: "PROBE INVALID: it did not discriminate between the two designs.\n");
	}

	static String describe(ConnectionInfo info) {
		return info.getHostName() + ':' + info.getHostPort();
	}

	// --- probe 3: does H2's per-stream :authority reach the same code? -----------------------

	static void probeHttp2AuthorityMapping() throws Exception {
		System.out.println("=== probe 3: HTTP/2 :authority -> Host, per stream ===");
		for (String host : HOSTS) {
			Http2Headers headers = new DefaultHttp2Headers()
					.method("GET").path("/probe").scheme("http").authority(host);
			HttpRequest request = HttpConversionUtil.toHttpRequest(3, headers, true);
			ConnectionInfo info = ConnectionInfo.from(request, false,
					new InetSocketAddress("127.0.0.1", 8080),
					new InetSocketAddress("127.0.0.1", 54321), null);
			System.out.printf(":authority %-22s -> Host %-22s -> %s%n",
					host, request.headers().get(HttpHeaderNames.HOST), describe(info));
		}
	}
}
