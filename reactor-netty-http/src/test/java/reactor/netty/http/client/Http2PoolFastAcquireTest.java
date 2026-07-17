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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.internal.shaded.reactor.pool.PoolBuilder;
import reactor.netty.internal.shaded.reactor.pool.PoolConfig;
import reactor.netty.internal.shaded.reactor.pool.PoolShutdownException;
import reactor.netty.internal.shaded.reactor.pool.PooledRef;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Deterministic interleaving tests for {@link Http2Pool}'s rotating fast-acquire path
 * (the {@code -Dreactor.netty.pool.h2.fastAcquire} kill switch, default off).
 *
 * <p>These pin the load-bearing invariants of the fast path: offer-before-execute, get()-drop
 * without re-offer, nothing fallible between poll and offer, the on-loop {@code pendingSize}
 * rescue re-check, the R2 conditional-removal fix, and the D1 rollback hardening. Each interleaving
 * is reproduced single-threaded with a stepping {@link EmbeddedChannel} plus a queue that injects a
 * concurrent action into the poll&rarr;offer window, so there is no scheduling flakiness.
 */
class Http2PoolFastAcquireTest {

	static final String FAST_ACQUIRE_PROP = "reactor.netty.pool.h2.fastAcquire";
	static final Duration ONE_SECOND = Duration.ofSeconds(1);

	// ---- happy path -------------------------------------------------------------------------

	@Test
	void happyMultiplexedFastDelivers() {
		EmbeddedChannel channel = newHttp2Channel();
		Http2Pool pool = buildPool(justConnection(channel), 1, 2, true);
		try {
			// Warm: first acquire allocates on the slow path (empty pool) and holds one stream.
			PooledRef<Connection> held = pool.acquire().block(ONE_SECOND);
			channel.runPendingTasks();
			assertThat(held).isNotNull();
			Http2Pool.Slot slot = slotOf(held);
			assertThat(pool.fastAcquireDelivered).as("warm-up used the slow path").isZero();

			// Second acquire multiplexes onto the warm connection through the fast path.
			List<PooledRef<Connection>> acquired = new ArrayList<>();
			pool.acquire().subscribe(acquired::add);
			assertThat(acquired).as("fastDeliver is queued, not yet run").isEmpty();

			channel.runPendingTasks();

			assertThat(acquired).hasSize(1);
			assertThat(pool.fastAcquireDelivered).as("served by the fast path").isEqualTo(1);
			assertThat(pool.activeStreams()).as("ACQUIRED counts both streams").isEqualTo(2);
			assertThat(slot.concurrency()).as("both streams on the one connection").isEqualTo(2);
			assertThat(pool.idleSize()).as("slot never left the queue (deliver(.., true))")
					.isEqualTo(pool.connections.size()).isEqualTo(1);

			held.release().block(ONE_SECOND);
			acquired.get(0).release().block(ONE_SECOND);
		}
		finally {
			cleanup(channel);
		}
	}

	@ParameterizedTest
	@ValueSource(ints = {1, 2})
	void happyReuseAfterReleaseFastDelivers(int maxConcurrentStreams) {
		EmbeddedChannel channel = newHttp2Channel();
		Http2Pool pool = buildPool(justConnection(channel), 1, maxConcurrentStreams, true);
		try {
			Http2Pool.Slot slot = warmOneIdleConnection(pool, channel);
			assertThat(pool.fastAcquireDelivered).isZero();
			assertThat(slot.concurrency()).as("idle after release").isZero();

			List<PooledRef<Connection>> acquired = new ArrayList<>();
			pool.acquire().subscribe(acquired::add);
			channel.runPendingTasks();

			assertThat(acquired).hasSize(1);
			assertThat(pool.fastAcquireDelivered).isEqualTo(1);
			assertThat(pool.activeStreams()).isEqualTo(1);
			assertThat(slot.concurrency()).isEqualTo(1);
			assertThat(pool.idleSize()).isEqualTo(pool.connections.size()).isEqualTo(1);

			acquired.get(0).release().block(ONE_SECOND);
		}
		finally {
			cleanup(channel);
		}
	}

	// ---- off-state (kill switch) ------------------------------------------------------------

	@Test
	void offStateNeverUsesFastPath() {
		EmbeddedChannel channel = newHttp2Channel();
		Http2Pool pool = buildPool(justConnection(channel), 1, 2, false);
		try {
			PooledRef<Connection> held = pool.acquire().block(ONE_SECOND);
			channel.runPendingTasks();
			assertThat(held).isNotNull();

			List<PooledRef<Connection>> acquired = new ArrayList<>();
			pool.acquire().subscribe(acquired::add);
			channel.runPendingTasks();

			assertThat(acquired).as("still delivered, via the slow path").hasSize(1);
			assertThat(pool.fastAcquireDelivered).as("fast path never taken with the flag off").isZero();
			assertThat(pool.activeStreams()).isEqualTo(2);

			held.release().block(ONE_SECOND);
			acquired.get(0).release().block(ONE_SECOND);
		}
		finally {
			cleanup(channel);
		}
	}

	// ---- rotation / round-robin -------------------------------------------------------------

	@Test
	void multiConnectionRoundRobinSpread() {
		ConcurrentLinkedQueue<EmbeddedChannel> channels = new ConcurrentLinkedQueue<>();
		Http2Pool pool = buildPool(suppliedConnections(channels), 2, 1, true);
		try {
			warmTwoIdleConnections(pool, channels);
			assertThat(pool.allocatedSize()).as("two connections warmed").isEqualTo(2);

			// Two fast acquires, both queued before either fastDeliver runs.
			CapturingSubscriber c = new CapturingSubscriber();
			CapturingSubscriber d = new CapturingSubscriber();
			assertThat(pool.fastAcquire(borrower(pool, c))).isTrue();
			assertThat(pool.fastAcquire(borrower(pool, d))).isTrue();

			channels.forEach(EmbeddedChannel::runPendingTasks);

			assertThat(c.value.get()).isNotNull();
			assertThat(d.value.get()).isNotNull();
			assertThat(c.value.get().poolable().channel())
					.as("head-poll/tail-offer rotation spreads the two claims across connections")
					.isNotSameAs(d.value.get().poolable().channel());
			assertThat(pool.fastAcquireDelivered).isEqualTo(2);
			assertThat(pool.allocatedSize()).as("no spurious allocation").isEqualTo(2);
			assertThat(pool.idleSize()).isEqualTo(pool.connections.size());

			c.value.get().release().block(ONE_SECOND);
			d.value.get().release().block(ONE_SECOND);
		}
		finally {
			cleanup(channels);
		}
	}

	// ---- round-robin spread (the product requirement) ---------------------------------------

	@Test
	void roundRobinSpreadMatchesSlowPath() {
		// The fast path must spread streams across connections exactly as the slow path does; a
		// peek/fill-first shortcut would pile every stream on the head connection. Four warm
		// connections, repeated rounds of one acquire-per-connection: the per-connection stream
		// counts must be uniform and identical for the fast path and the slow path.
		List<Integer> fastPath = measureRoundRobinSpread(true);
		List<Integer> slowPath = measureRoundRobinSpread(false);

		assertThat(fastPath).as("fast path spreads uniformly across the 4 connections")
				.containsExactly(ROUNDS, ROUNDS, ROUNDS, ROUNDS);
		assertThat(fastPath).as("fast-path spread matches the slow path in shape").isEqualTo(slowPath);
	}

	static final int CONNECTIONS = 4;
	static final int ROUNDS = 5;

	static List<Integer> measureRoundRobinSpread(boolean fastAcquire) {
		ConcurrentLinkedQueue<EmbeddedChannel> channels = new ConcurrentLinkedQueue<>();
		Http2Pool pool = buildPool(suppliedConnections(channels), CONNECTIONS, 1, fastAcquire);
		try {
			warmConnections(pool, channels, CONNECTIONS);
			assertThat(pool.allocatedSize()).isEqualTo(CONNECTIONS);

			Map<io.netty.channel.Channel, Integer> perConnection = new IdentityHashMap<>();
			for (int round = 0; round < ROUNDS; round++) {
				List<CapturingSubscriber> subs = new ArrayList<>();
				for (int i = 0; i < CONNECTIONS; i++) {
					CapturingSubscriber sub = new CapturingSubscriber();
					if (fastAcquire) {
						pool.fastAcquire(borrower(pool, sub));
					}
					else {
						pool.doAcquire(borrower(pool, sub));
					}
					subs.add(sub);
				}
				channels.forEach(EmbeddedChannel::runPendingTasks);

				List<PooledRef<Connection>> held = new ArrayList<>();
				for (CapturingSubscriber sub : subs) {
					Http2Pool.Http2PooledRef ref = sub.value.get();
					assertThat(ref).isNotNull();
					perConnection.merge(ref.poolable().channel(), 1, Integer::sum);
					held.add(ref);
				}
				for (PooledRef<Connection> ref : held) {
					ref.release().block(ONE_SECOND);
				}
				channels.forEach(EmbeddedChannel::runPendingTasks);
			}

			List<Integer> counts = new ArrayList<>(perConnection.values());
			Collections.sort(counts);
			return counts;
		}
		finally {
			cleanup(channels);
		}
	}

	// ---- window rescue (offer-before-execute + on-loop pendingSize re-check) -----------------

	@Test
	void windowRescueBothServed() {
		EmbeddedChannel channel = newHttp2Channel();
		Http2Pool pool = buildPool(justConnection(channel), 1, 2, true);
		try {
			warmOneIdleConnection(pool, channel);

			// A borrower that arrives inside the poll->offer window: it misses the invisible slot
			// and pends (permits exhausted). Injected single-threaded, right after the fast path
			// dequeues the head and before it offers back -- exactly case (ii) of the rescue Lemma.
			CapturingSubscriber pending = new CapturingSubscriber();
			WindowQueue<Http2Pool.Slot> window = installWindowQueue(pool);
			window.afterFirstPoll = () -> pool.doAcquire(borrower(pool, pending));

			CapturingSubscriber fast = new CapturingSubscriber();
			boolean fastAccepted = pool.fastAcquire(borrower(pool, fast));

			assertThat(fastAccepted).as("fast path offered the slot back and queued fastDeliver").isTrue();
			assertThat(pool.pendingSize).as("window borrower pended during the window").isEqualTo(1);
			assertThat(pending.value.get()).as("window borrower not served yet").isNull();

			channel.runPendingTasks();

			assertThat(pending.value.get()).as("window borrower rescued").isNotNull();
			assertThat(fast.value.get()).as("fast borrower re-served behind it").isNotNull();
			assertThat(pool.fastAcquireDelivered)
					.as("the on-loop re-check aborted the fast claim to the slow path").isZero();
			assertThat(pool.activeStreams()).isEqualTo(2);
			assertThat(pool.idleSize()).isEqualTo(pool.connections.size());

			pending.value.get().release().block(ONE_SECOND);
			fast.value.get().release().block(ONE_SECOND);
		}
		finally {
			cleanup(channel);
		}
	}

	@Test
	void windowOfferBeforeExecuteIsLoadBearing() {
		EmbeddedChannel channel = newHttp2Channel();
		Http2Pool pool = buildPool(justConnection(channel), 1, 2, true);
		try {
			warmOneIdleConnection(pool, channel);

			// Same window as the rescue test, but the hook runs the loop tasks the instant the slot
			// is offered back. With offer-before-execute (correct), fastDeliver is not yet queued, so
			// the hook is a no-op. If the offer were moved after the execute, fastDeliver would run
			// mid-window -- before the slot is visible -- and both borrowers would strand. This pins
			// the ordering invariant single-threaded.
			CapturingSubscriber pending = new CapturingSubscriber();
			WindowQueue<Http2Pool.Slot> window = installWindowQueue(pool);
			window.afterFirstPoll = () -> pool.doAcquire(borrower(pool, pending));
			window.beforeFirstOffer = channel::runPendingTasks;

			CapturingSubscriber fast = new CapturingSubscriber();
			pool.fastAcquire(borrower(pool, fast));
			channel.runPendingTasks();

			assertThat(pending.value.get()).as("window borrower served").isNotNull();
			assertThat(fast.value.get()).as("fast borrower served").isNotNull();
			assertThat(pool.activeStreams()).isEqualTo(2);
			assertThat(pool.idleSize()).isEqualTo(pool.connections.size());

			pending.value.get().release().block(ONE_SECOND);
			fast.value.get().release().block(ONE_SECOND);
		}
		finally {
			cleanup(channel);
		}
	}

	// ---- zombie handling --------------------------------------------------------------------

	@Test
	void zombieHeadDroppedNotReoffered() {
		EmbeddedChannel channel = newHttp2Channel();
		Http2Pool pool = buildPool(justConnection(channel), 1, 2, true);
		try {
			Http2Pool.Slot slot = warmOneIdleConnection(pool, channel);
			slot.invalidate();
			assertThat(slot.get()).as("retired zombie").isTrue();
			assertThat(pool.connections).containsExactly(slot);
			assertThat(pool.idleSize()).isEqualTo(1);

			CapturingSubscriber sub = new CapturingSubscriber();
			boolean fastAccepted = pool.fastAcquire(borrower(pool, sub));

			assertThat(fastAccepted).as("zombie head not served by the fast path").isFalse();
			assertThat(pool.connections).as("zombie dropped, not re-offered").doesNotContain(slot);
			assertThat(pool.connections).isEmpty();
			assertThat(pool.idleSize()).as("one poll, no re-offer").isZero();
			assertThat(pool.fastAcquireDelivered).isZero();
		}
		finally {
			cleanup(channel);
		}
	}

	@Test
	void zombieReofferedDuringWindowIsSkipped() {
		ConcurrentLinkedQueue<EmbeddedChannel> channels = new ConcurrentLinkedQueue<>();
		Http2Pool pool = buildPool(suppliedConnections(channels), 1, 1, true);
		try {
			PooledRef<Connection> warm = pool.acquire().block(ONE_SECOND);
			channels.forEach(EmbeddedChannel::runPendingTasks);
			Http2Pool.Slot slot = slotOf(warm);
			warm.release().block(ONE_SECOND);
			channels.forEach(EmbeddedChannel::runPendingTasks);
			EmbeddedChannel first = channels.peek();

			// Retire the polled head just before the fast path offers it back: it re-enters the
			// queue as a zombie (maxConcurrentStreams now 0), so the fast path bails and the slow
			// path drops it and allocates a fresh connection.
			WindowQueue<Http2Pool.Slot> window = installWindowQueue(pool);
			window.beforeFirstOffer = slot::invalidate;

			CapturingSubscriber sub = new CapturingSubscriber();
			pool.doAcquire(borrower(pool, sub));
			channels.forEach(EmbeddedChannel::runPendingTasks);

			assertThat(sub.value.get()).as("borrower served").isNotNull();
			assertThat(sub.value.get().poolable().channel())
					.as("served on a fresh connection, not the zombie").isNotSameAs(first);
			assertThat(pool.connections).as("zombie dropped by the slow path").doesNotContain(slot);
			assertThat(pool.fastAcquireDelivered).isZero();
			assertThat(pool.idleSize()).as("idle-size conserved").isEqualTo(pool.connections.size());
			assertThat(channels).as("exactly one extra connection allocated").hasSize(2);

			sub.value.get().release().block(ONE_SECOND);
		}
		finally {
			cleanup(channels);
		}
	}

	// ---- R2: evict tick racing a fast poll on a dying head ----------------------------------

	@Test
	void evictRaceOnDyingHeadConservesIdleSize() throws Exception {
		EmbeddedChannel channel = newHttp2Channel();
		Http2Pool pool = buildPool(justConnection(channel), 1, 2, false);
		try {
			Http2Pool.Slot slot = warmOneIdleConnection(pool, channel);
			// The head is dying: GO_AWAY received, still idle (concurrency 0).
			Http2FrameCodec frameCodec = channel.pipeline().get(Http2FrameCodec.class);
			frameCodec.connection().goAwayReceived(Integer.MAX_VALUE, 0L, Unpooled.EMPTY_BUFFER);
			assertThat(slot.goAwayReceived()).isTrue();

			WindowQueue<Http2Pool.Slot> window = installWindowQueue(pool);
			window.stickyIterator = true;

			// Fast path polls the dying head out of the queue (idle -1)...
			Http2Pool.Slot popped = pool.pollSlot(window);
			assertThat(popped).isSameAs(slot);
			assertThat(pool.idleSize()).isZero();

			// ...an evict tick still sees it (the CLQ iterator returns a concurrently-dequeued node)
			// and tries to remove it. resources.remove() misses, so the conditional fix skips the
			// decrement -- pre-R2 this decremented unconditionally and drifted idleSize.
			pool.evictInBackground();

			// The conditional skip also left the slot untouched: not invalidated, permit retained.
			assertThat(slot.get()).as("evict's remove() missed, so invalidate was skipped").isFalse();
			assertThat(pool.allocatedSize()).as("permit not spuriously returned").isEqualTo(1);

			// Fast path offers the slot back (idle +1).
			pool.offerSlot(window, popped);

			assertThat(pool.idleSize())
					.as("idleSize conserved against the evict race")
					.isEqualTo(pool.connections.size()).isEqualTo(1);
		}
		finally {
			cleanup(channel);
		}
	}

	// ---- dispose during the window ----------------------------------------------------------

	@Test
	void disposeDuringWindowFailsBorrower() {
		EmbeddedChannel channel = newHttp2Channel();
		Http2Pool pool = buildPool(justConnection(channel), 1, 2, true);
		try {
			warmOneIdleConnection(pool, channel);

			CapturingSubscriber sub = new CapturingSubscriber();
			boolean fastAccepted = pool.fastAcquire(borrower(pool, sub));
			assertThat(fastAccepted).as("fastDeliver queued").isTrue();

			pool.disposeLater().block(ONE_SECOND);
			channel.runPendingTasks();

			assertThat(sub.error.get())
					.as("fastDeliver on a disposed pool fails rather than delivering")
					.isInstanceOf(PoolShutdownException.class);
			assertThat(sub.value.get()).isNull();
			assertThat(Http2Pool.TERMINATED).as("static sentinel not polluted")
					.doesNotContain(borrowerRef(sub));
		}
		finally {
			cleanup(channel);
		}
	}

	// ---- D1: deliver() rollback on a disposed pool ------------------------------------------

	@Test
	void deliverRollbackOnDisposedPoolFailsBorrower() {
		EmbeddedChannel channel = newHttp2Channel();
		Http2Pool pool = buildPool(justConnection(channel), 1, 1, false);
		try {
			// Acquire the single permitted stream to obtain a slot, then dispose: the slot is retired
			// and pending becomes the static TERMINATED sentinel.
			Http2Pool.Http2PooledRef ref = (Http2Pool.Http2PooledRef) pool.acquire().block(ONE_SECOND);
			assertThat(ref).isNotNull();
			pool.disposeLater().block(ONE_SECOND);

			// deliver() into the retired slot fails canOpenStream() and takes the rollback branch,
			// which must fail the borrower rather than re-pend it onto the static sentinel.
			CapturingSubscriber sub = new CapturingSubscriber();
			Http2Pool.Borrower borrower = new Http2Pool.Borrower(sub, pool, Duration.ZERO);
			borrower.deliver(ref, false);

			assertThat(sub.error.get())
					.as("borrower failed rather than stranded")
					.isInstanceOf(PoolShutdownException.class);
			assertThat(pool.pendingSize).as("not re-pended").isZero();
			assertThat(Http2Pool.TERMINATED).as("static sentinel not polluted").doesNotContain(borrower);
		}
		finally {
			cleanup(channel);
		}
	}

	// ---- D1: deliver() rollback arms the timeout and drains ---------------------------------

	@Test
	void deliverRollbackArmsTimeoutAndDrains() {
		ConcurrentLinkedQueue<EmbeddedChannel> channels = new ConcurrentLinkedQueue<>();
		AtomicInteger timerArmed = new AtomicInteger();
		BiFunction<Runnable, Duration, Disposable> timer = (r, d) -> {
			timerArmed.incrementAndGet();
			return Disposables.disposed();
		};

		String previous = System.getProperty(FAST_ACQUIRE_PROP);
		System.setProperty(FAST_ACQUIRE_PROP, "false");
		Http2Pool pool;
		try {
			PoolBuilder<Connection, PoolConfig<Connection>> poolBuilder =
					PoolBuilder.from(suppliedConnections(channels))
					           .idleResourceReuseLruOrder()
					           .maxPendingAcquireUnbounded()
					           .pendingAcquireTimer(timer)
					           .sizeBetween(0, 2);
			Http2AllocationStrategy strategy = Http2AllocationStrategy.builder()
					.maxConnections(2)
					.maxConcurrentStreams(1)
					.build();
			pool = poolBuilder.build(config -> new Http2Pool(config, strategy));
		}
		finally {
			if (previous == null) {
				System.clearProperty(FAST_ACQUIRE_PROP);
			}
			else {
				System.setProperty(FAST_ACQUIRE_PROP, previous);
			}
		}

		try {
			warmTwoIdleConnections(pool, channels);
			Http2Pool.Slot target = pool.connections.peek();
			assertThat(target).isNotNull();

			// Simulate a reserved claim on the head, then an off-loop invalidate landing before the
			// on-loop deliver -- the one race that reaches deliver()'s rollback for a claim that never
			// ran pendingOffer (so it armed no timeout and scheduled no drain of its own).
			Http2Pool.ACQUIRED.incrementAndGet(pool);
			target.incrementConcurrencyAndGet();
			target.invalidate();

			CapturingSubscriber sub = new CapturingSubscriber();
			Http2Pool.Borrower borrower = new Http2Pool.Borrower(sub, pool, Duration.ofSeconds(30));
			borrower.deliver(new Http2Pool.Http2PooledRef(target), true);

			assertThat(target.concurrency()).as("reserved claim rolled back").isZero();
			assertThat(timerArmed.get())
					.as("rollback armed the pending-acquire timeout for a borrower that never pended")
					.isEqualTo(1);

			// The rollback's drain re-serves the borrower on the surviving idle connection.
			channels.forEach(EmbeddedChannel::runPendingTasks);
			assertThat(sub.value.get()).as("re-pended borrower re-served by the rollback drain").isNotNull();
			assertThat(sub.value.get().poolable().channel())
					.as("served on the surviving connection, not the retired one")
					.isNotSameAs(target.connection.channel());

			sub.value.get().release().block(ONE_SECOND);
		}
		finally {
			cleanup(channels);
		}
	}

	// ---- helpers ----------------------------------------------------------------------------

	static EmbeddedChannel newHttp2Channel() {
		return new EmbeddedChannel(
				Http2FrameCodecBuilder.forClient().build(),
				new Http2MultiplexHandler(new ChannelHandlerAdapter() {}));
	}

	static Mono<Connection> justConnection(EmbeddedChannel channel) {
		return Mono.just(Connection.from(channel));
	}

	static Mono<Connection> suppliedConnections(ConcurrentLinkedQueue<EmbeddedChannel> channels) {
		return Mono.fromSupplier(() -> {
			EmbeddedChannel channel = newHttp2Channel();
			channels.add(channel);
			return Connection.from(channel);
		});
	}

	static Http2Pool buildPool(Mono<Connection> allocator, int maxConnections, int maxConcurrentStreams,
			boolean fastAcquire) {
		String previous = System.getProperty(FAST_ACQUIRE_PROP);
		System.setProperty(FAST_ACQUIRE_PROP, Boolean.toString(fastAcquire));
		try {
			PoolBuilder<Connection, PoolConfig<Connection>> poolBuilder =
					PoolBuilder.from(allocator)
					           .idleResourceReuseLruOrder()
					           .maxPendingAcquireUnbounded()
					           .sizeBetween(0, maxConnections);
			Http2AllocationStrategy strategy = Http2AllocationStrategy.builder()
					.maxConnections(maxConnections)
					.maxConcurrentStreams(maxConcurrentStreams)
					.build();
			return poolBuilder.build(config -> new Http2Pool(config, strategy));
		}
		finally {
			if (previous == null) {
				System.clearProperty(FAST_ACQUIRE_PROP);
			}
			else {
				System.setProperty(FAST_ACQUIRE_PROP, previous);
			}
		}
	}

	static Http2Pool.Slot warmOneIdleConnection(Http2Pool pool, EmbeddedChannel channel) {
		PooledRef<Connection> ref = pool.acquire().block(ONE_SECOND);
		channel.runPendingTasks();
		Http2Pool.Slot slot = slotOf(ref);
		ref.release().block(ONE_SECOND);
		channel.runPendingTasks();
		return slot;
	}

	static void warmConnections(Http2Pool pool, ConcurrentLinkedQueue<EmbeddedChannel> channels, int count) {
		// maxConcurrentStreams == 1 forces each acquire to allocate a fresh connection.
		List<PooledRef<Connection>> held = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			held.add(pool.acquire().block(ONE_SECOND));
		}
		channels.forEach(EmbeddedChannel::runPendingTasks);
		for (PooledRef<Connection> ref : held) {
			ref.release().block(ONE_SECOND);
		}
		channels.forEach(EmbeddedChannel::runPendingTasks);
	}

	static void warmTwoIdleConnections(Http2Pool pool, ConcurrentLinkedQueue<EmbeddedChannel> channels) {
		// maxConcurrentStreams == 1 forces the second acquire to allocate a second connection.
		PooledRef<Connection> a = pool.acquire().block(ONE_SECOND);
		PooledRef<Connection> b = pool.acquire().block(ONE_SECOND);
		channels.forEach(EmbeddedChannel::runPendingTasks);
		a.release().block(ONE_SECOND);
		b.release().block(ONE_SECOND);
		channels.forEach(EmbeddedChannel::runPendingTasks);
	}

	static Http2Pool.Slot slotOf(PooledRef<Connection> ref) {
		return ((Http2Pool.Http2PooledRef) ref).slot;
	}

	static Http2Pool.Borrower borrower(Http2Pool pool, CapturingSubscriber subscriber) {
		Http2Pool.Borrower borrower = new Http2Pool.Borrower(subscriber, pool, Duration.ZERO);
		subscriber.borrower = borrower;
		return borrower;
	}

	static Http2Pool.Borrower borrowerRef(CapturingSubscriber subscriber) {
		return subscriber.borrower;
	}

	@SuppressWarnings("unchecked")
	static WindowQueue<Http2Pool.Slot> installWindowQueue(Http2Pool pool) {
		WindowQueue<Http2Pool.Slot> window = new WindowQueue<>();
		window.addAll(pool.connections);
		Http2Pool.CONNECTIONS.set(pool, window);
		return window;
	}

	static void cleanup(EmbeddedChannel channel) {
		channel.finishAndReleaseAll();
		Connection.from(channel).dispose();
	}

	static void cleanup(ConcurrentLinkedQueue<EmbeddedChannel> channels) {
		for (EmbeddedChannel channel : channels) {
			channel.finishAndReleaseAll();
			Connection.from(channel).dispose();
		}
	}

	/**
	 * A {@link ConcurrentLinkedQueue} that injects a deterministic action into the fast path's
	 * poll&rarr;offer window and can model the JDK iterator returning a concurrently-dequeued node.
	 */
	static final class WindowQueue<T> extends ConcurrentLinkedQueue<T> {

		@Nullable Runnable afterFirstPoll;
		@Nullable Runnable beforeFirstOffer;
		boolean stickyIterator;

		private boolean polled;
		private boolean offered;
		private @Nullable T captured;

		@Override
		public @Nullable T poll() {
			T value = super.poll();
			if (!polled) {
				polled = true;
				captured = value;
				if (afterFirstPoll != null) {
					afterFirstPoll.run();
				}
			}
			return value;
		}

		@Override
		public boolean offer(T t) {
			if (!offered) {
				offered = true;
				if (beforeFirstOffer != null) {
					beforeFirstOffer.run();
				}
			}
			return super.offer(t);
		}

		@Override
		public Iterator<T> iterator() {
			T sticky = captured;
			if (stickyIterator && sticky != null) {
				// Models ConcurrentLinkedQueue.Itr: "once we claim that an element exists in
				// hasNext(), we must return it" -- the iterator yields a node whose item was
				// concurrently removed. remove() is a void no-op, as CLQ.Itr.remove() is on a
				// node already unlinked from the queue.
				return new Iterator<T>() {
					boolean hasNext = true;

					@Override
					public boolean hasNext() {
						return hasNext;
					}

					@Override
					public T next() {
						if (!hasNext) {
							throw new NoSuchElementException();
						}
						hasNext = false;
						return sticky;
					}

					@Override
					public void remove() {
					}
				};
			}
			return super.iterator();
		}
	}

	static final class CapturingSubscriber implements CoreSubscriber<Http2Pool.Http2PooledRef> {

		final AtomicReference<Throwable> error = new AtomicReference<>();
		final AtomicReference<Http2Pool.Http2PooledRef> value = new AtomicReference<>();
		Http2Pool.@Nullable Borrower borrower;

		@Override
		public void onSubscribe(Subscription s) {
		}

		@Override
		public void onNext(Http2Pool.Http2PooledRef ref) {
			value.set(ref);
		}

		@Override
		public void onError(Throwable t) {
			error.set(t);
		}

		@Override
		public void onComplete() {
		}
	}
}
