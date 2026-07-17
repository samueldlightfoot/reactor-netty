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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.Connection;
import reactor.netty.internal.shaded.reactor.pool.PoolBuilder;
import reactor.netty.internal.shaded.reactor.pool.PoolConfig;
import reactor.netty.internal.shaded.reactor.pool.PooledRef;

/**
 * Microbenchmarks for the per-request cost of {@link Http2Pool}.
 *
 * <p>Two benchmark shapes (see {@code tasks/http2-pool-perf/findings.md} §5):
 * <ul>
 *     <li>{@link #acquireRelease}/{@link #acquireReleaseMultiplexed} run a full
 *     acquire &rarr; deliver &rarr; release cycle over an {@link EmbeddedChannel}. This captures the
 *     drain-path fixed overhead (clock calls, atomics, the h2c-upgrade pipeline lookups). An
 *     EmbeddedChannel is adequate here because these paths do not depend on {@code inEventLoop()}.</li>
 *     <li>{@link #goAwayReceivedOffLoop} calls {@code goAwayReceived()} from a thread that is NOT the
 *     channel's event loop, over a real single-threaded loop. This is the only way to exercise the
 *     off-loop pipeline walk targeted by OPT-1 — {@code EmbeddedEventLoop.inEventLoop()} is hardcoded
 *     {@code true}, so an EmbeddedChannel would always take the cached fast path and show no delta.</li>
 * </ul>
 */
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class Http2PoolBenchmark {

	@State(Scope.Thread)
	public static class EmbeddedState {

		EmbeddedChannel channel;
		Http2Pool pool;
		Http2Pool.Slot slot;
		final AtomicReference<PooledRef<Connection>> ref = new AtomicReference<>();
		PooledRef<Connection> held; // for the multiplexed benchmark: one stream kept active

		@Setup(Level.Trial)
		public void setup() {
			channel = new EmbeddedChannel(Http2FrameCodecBuilder.forClient().build(),
					new Http2MultiplexHandler(new ChannelHandlerAdapter() {}));
			channel.pipeline().get(Http2FrameCodec.class)
			       .connection().local().maxActiveStreams(Integer.MAX_VALUE);

			Http2AllocationStrategy strategy = Http2AllocationStrategy.builder()
					.maxConnections(1)
					.maxConcurrentStreams(200)
					.build();
			PoolBuilder<Connection, PoolConfig<Connection>> poolBuilder =
					PoolBuilder.from(Mono.just(Connection.from(channel)))
					           .idleResourceReuseLruOrder()
					           .maxPendingAcquireUnbounded()
					           .sizeBetween(0, 1);
			pool = poolBuilder.build(config -> new Http2Pool(config, strategy));

			// Allocate the single connection once so every measured iteration hits the reuse path.
			PooledRef<Connection> warm = pool.acquire().block();
			channel.runPendingTasks();
			warm.invalidate().block();
			channel.runPendingTasks();

			// One long-lived stream so releases in acquireReleaseMultiplexed never bring the
			// connection to idle (concurrency never reaches 0).
			pool.acquire().subscribe(ref::set);
			channel.runPendingTasks();
			held = ref.get();
			slot = ((Http2Pool.Http2PooledRef) held).slot;
		}

		@TearDown(Level.Trial)
		public void tearDown() {
			channel.finishAndReleaseAll();
			Connection.from(channel).dispose();
		}

		PooledRef<Connection> acquireOne() {
			ref.set(null);
			pool.acquire().subscribe(ref::set);
			channel.runPendingTasks();
			return ref.get();
		}
	}

	@Benchmark
	public void acquireRelease(EmbeddedState state, Blackhole bh) {
		PooledRef<Connection> r = state.acquireOne();
		bh.consume(r);
		r.invalidate().subscribe();
		state.channel.runPendingTasks();
	}

	@Benchmark
	public void acquireReleaseMultiplexed(EmbeddedState state, Blackhole bh) {
		// state.held keeps concurrency >= 1, so this release never reaches idle (exercises OPT-8).
		PooledRef<Connection> r = state.acquireOne();
		bh.consume(r);
		r.invalidate().subscribe();
		state.channel.runPendingTasks();
	}

	@Benchmark
	public void emptyDrain(EmbeddedState state) {
		// No pending borrowers: the empty drainLoop that OPT-3b strips of its clock reads
		// (this is ~3 of the ~4 drainLoop entries per request in production).
		state.pool.drain();
	}

	@Benchmark
	public boolean isH2cUpgrade(EmbeddedState state) {
		// Direct-H2 slot with no upgrade handler: the baseline re-walks the pipeline on every call,
		// the patched version caches the negative lookup (OPT-7).
		return state.slot.isH2cUpgrade();
	}

	@State(Scope.Thread)
	public static class RealLoopState {

		DefaultEventLoopGroup group;
		LocalChannel channel;
		Http2Pool.Slot slot;

		@Setup(Level.Trial)
		public void setup() throws Exception {
			group = new DefaultEventLoopGroup(1);
			channel = new LocalChannel();
			group.register(channel).sync();
			group.submit(() -> channel.pipeline().addLast(
					Http2FrameCodecBuilder.forClient().build(),
					new Http2MultiplexHandler(new ChannelHandlerAdapter() {}))).sync();

			Connection connection = Connection.from(channel);
			PoolBuilder<Connection, PoolConfig<Connection>> poolBuilder =
					PoolBuilder.from(Mono.just(connection))
					           .idleResourceReuseLruOrder()
					           .maxPendingAcquireUnbounded()
					           .sizeBetween(0, 1);
			Http2Pool pool = poolBuilder.build(config -> new Http2Pool(config, null));
			// Slot construction runs on the event loop (initMaxConcurrentStreams asserts inEventLoop).
			slot = group.submit(() -> pool.createSlot(connection)).get();
		}

		@TearDown(Level.Trial)
		public void tearDown() throws Exception {
			channel.close().sync();
			group.shutdownGracefully().sync();
		}
	}

	@Benchmark
	public boolean goAwayReceivedOffLoop(RealLoopState state) {
		// Runs on the JMH thread, which is NOT the channel's event loop: the baseline walks the
		// pipeline here, the patched version reads the memoized Http2Connection.
		return state.slot.goAwayReceived();
	}

	/**
	 * A warm, in-use pool: one connection multiplexing several concurrent streams, over a real
	 * event loop. Acquire runs off-loop (the drainLoop/findConnection scan), delivery and release
	 * run on the event loop — exactly the production threading. This is the realistic common case.
	 */
	@State(Scope.Benchmark)
	public static class UsedPoolState {

		static final int HELD_STREAMS = 8;

		EventLoopGroup group;
		Channel server;
		Http2Pool pool;
		final List<PooledRef<Connection>> held = new ArrayList<>();

		@Setup(Level.Trial)
		public void setup() throws Exception {
			group = new DefaultEventLoopGroup(2);
			LocalAddress address = new LocalAddress("http2-pool-benchmark");
			server = new ServerBootstrap()
					.group(group)
					.channel(LocalServerChannel.class)
					.childHandler(new ChannelHandlerAdapter() {})
					.bind(address)
					.sync()
					.channel();

			Channel channel = new Bootstrap()
					.group(group)
					.channel(LocalChannel.class)
					.handler(new ChannelInitializer<Channel>() {
						@Override
						protected void initChannel(Channel ch) {
							ch.pipeline().addLast(Http2FrameCodecBuilder.forClient().build(),
									new Http2MultiplexHandler(new ChannelHandlerAdapter() {}));
						}
					})
					.connect(address)
					.sync()
					.channel();
			channel.eventLoop().submit(() ->
					channel.pipeline().get(Http2FrameCodec.class)
					       .connection().local().maxActiveStreams(Integer.MAX_VALUE)).sync();

			Connection connection = Connection.from(channel);
			Http2AllocationStrategy strategy = Http2AllocationStrategy.builder()
					.maxConnections(1)
					.maxConcurrentStreams(200)
					.build();
			// The allocator must emit on the event loop (as a real connect does), so the first
			// deliver runs on-loop; subsequent acquires reuse the cached connection.
			Mono<Connection> allocator = Mono.just(connection)
					.subscribeOn(Schedulers.fromExecutor(channel.eventLoop()));
			PoolBuilder<Connection, PoolConfig<Connection>> poolBuilder =
					PoolBuilder.from(allocator)
					           .idleResourceReuseLruOrder()
					           .maxPendingAcquireUnbounded()
					           .sizeBetween(0, 1);
			pool = poolBuilder.build(config -> new Http2Pool(config, strategy));

			// Steady state: keep several streams multiplexed on the connection.
			for (int i = 0; i < HELD_STREAMS; i++) {
				held.add(pool.acquire().block());
			}
		}

		@TearDown(Level.Trial)
		public void tearDown() throws Exception {
			for (PooledRef<Connection> ref : held) {
				ref.invalidate().block();
			}
			server.close().sync();
			group.shutdownGracefully().sync();
		}
	}

	@Benchmark
	@Threads(4)
	public void acquireReleaseUsedPool(UsedPoolState state) {
		// Acquire a stream off-loop (drainLoop + findConnection), then deliver + release on the
		// event loop, while HELD_STREAMS streams stay multiplexed on the connection.
		state.pool.acquire().flatMap(PooledRef::invalidate).block();
	}
}
