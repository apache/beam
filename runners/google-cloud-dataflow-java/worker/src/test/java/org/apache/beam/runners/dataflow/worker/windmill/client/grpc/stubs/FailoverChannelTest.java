/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.CallCredentials;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ClientCall.Listener;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ConnectivityState;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class FailoverChannelTest {
  private MethodDescriptor<Object, Object> methodDescriptor =
      MethodDescriptor.newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(MethodDescriptor.generateFullMethodName("test", "test"))
          .setRequestMarshaller(new NoopClientCall.NoopMarshaller())
          .setResponseMarshaller(new NoopClientCall.NoopMarshaller())
          .build();

  private static FailoverChannel createForTest(ManagedChannel primary, ManagedChannel fallback) {
    return createForTest(primary, fallback, null, System::nanoTime, null);
  }

  private static FailoverChannel createForTest(
      ManagedChannel primary,
      ManagedChannel fallback,
      @Nullable CallCredentials fallbackCallCredentials,
      LongSupplier nanoClock) {
    return createForTest(primary, fallback, fallbackCallCredentials, nanoClock, null);
  }

  private static FailoverChannel createForTest(
      ManagedChannel primary,
      ManagedChannel fallback,
      @Nullable CallCredentials fallbackCallCredentials,
      LongSupplier nanoClock,
      @Nullable Long rpcFailureThresholdNanos) {
    return FailoverChannel.forTest(
        primary,
        fallback,
        fallbackCallCredentials,
        nanoClock,
        rpcFailureThresholdNanos != null ? rpcFailureThresholdNanos : 0L);
  }

  /**
   * Starts a call on the primary channel, captures the injected listener, and fires onClose with
   * the given status. Use this to trigger RPC-based failover in tests.
   */
  private void triggerRPCFailure(
      FailoverChannel channel, ClientCall<Object, Object> underlying, Status status)
      throws Exception {
    Metadata metadata = new Metadata();
    channel
        .newCall(methodDescriptor, CallOptions.DEFAULT)
        .start(new NoopClientCall.NoopClientCallListener<>(), metadata);
    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(underlying).start(captor.capture(), same(metadata));
    captor.getValue().onClose(status, new Metadata());
  }

  @Test
  public void testRPCFailureTriggersFallback() throws Exception {
    // RPC failure with UNAVAILABLE should switch to fallback channel.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall);
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));

    FailoverChannel failoverChannel = createForTest(mockChannel, mockFallbackChannel);

    triggerRPCFailure(failoverChannel, underlyingCall, Status.UNAVAILABLE);

    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockFallbackChannel, atLeastOnce()).newCall(any(), any());
  }

  @Test
  public void testRPCFallbackRespectsThirtySecondGracePeriod() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> firstUnderlyingCall = mock(ClientCall.class);
    ClientCall<Object, Object> secondUnderlyingCall = mock(ClientCall.class);
    ClientCall<Object, Object> thirdUnderlyingCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any()))
        .thenReturn(firstUnderlyingCall, secondUnderlyingCall, thirdUnderlyingCall);
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    when(mockChannel.getState(false)).thenReturn(ConnectivityState.READY);

    AtomicLong time = new AtomicLong(0);
    FailoverChannel failoverChannel =
        createForTest(
            mockChannel, mockFallbackChannel, null, time::get, TimeUnit.SECONDS.toNanos(30));

    // First failure at t=0 should not trigger fallback.
    triggerRPCFailure(failoverChannel, firstUnderlyingCall, Status.UNAVAILABLE);
    verify(mockFallbackChannel, never()).newCall(any(), any());

    // Second failure before 30s should still not trigger fallback.
    time.addAndGet(TimeUnit.SECONDS.toNanos(29));
    triggerRPCFailure(failoverChannel, secondUnderlyingCall, Status.UNAVAILABLE);
    verify(mockFallbackChannel, never()).newCall(any(), any());

    // Failure at/after 30s should trigger fallback.
    time.addAndGet(TimeUnit.SECONDS.toNanos(1));
    triggerRPCFailure(failoverChannel, thirdUnderlyingCall, Status.UNAVAILABLE);
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockFallbackChannel, atLeastOnce()).newCall(any(), any());
  }

  @Test
  public void testRPCFailureRecoveryAfterCoolingPeriod() throws Exception {
    // After RPC failure, channel stays on fallback during cooling period, then returns to primary.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall, mock(ClientCall.class));
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    when(mockChannel.getState(false)).thenReturn(ConnectivityState.READY);

    AtomicLong time = new AtomicLong(0);
    FailoverChannel failoverChannel =
        createForTest(mockChannel, mockFallbackChannel, null, time::get);

    triggerRPCFailure(failoverChannel, underlyingCall, Status.UNAVAILABLE);

    // Within cooling period, still on fallback
    time.addAndGet(TimeUnit.MINUTES.toNanos(30));
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockFallbackChannel, atLeastOnce()).newCall(any(), any());

    // After cooling period, recovers to primary
    time.addAndGet(TimeUnit.MINUTES.toNanos(40));
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockChannel, atLeast(2)).newCall(any(), any());
  }

  @Test
  public void testRPCFallbackClearedByConnectivityRecovery() throws Exception {
    // Race condition: RPC failure observed just before connectivity callback fires READY.
    // Once the channel goes through unhealthy→healthy, the cooling period must be cancelled
    // and traffic must return to primary immediately (not wait 1 hour).
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall, mock(ClientCall.class));
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));

    when(mockChannel.getState(false)).thenReturn(ConnectivityState.IDLE, ConnectivityState.READY);

    AtomicReference<Runnable> stateChangeCallback = new AtomicReference<>();
    doAnswer(
            invocation -> {
              stateChangeCallback.set(invocation.getArgument(1));
              return null;
            })
        .when(mockChannel)
        .notifyWhenStateChanged(any(), any());

    AtomicLong time = new AtomicLong(0);
    FailoverChannel failoverChannel =
        createForTest(mockChannel, mockFallbackChannel, null, time::get);

    // RPC failure results in entering cooling period
    triggerRPCFailure(failoverChannel, underlyingCall, Status.UNAVAILABLE);

    // Still within cooling period, routes to fallback
    time.addAndGet(TimeUnit.MINUTES.toNanos(30));
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockFallbackChannel, atLeastOnce()).newCall(any(), any());

    // Primary recovers and callback fires READY, clearing the cooling period
    stateChangeCallback.get().run();

    // Verify immediately routes back to primary
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockChannel, atLeast(2)).newCall(any(), any());
  }

  @Test
  public void testFallbackWithCredentials() throws Exception {
    // Fallback channel should receive custom credentials when provided.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    CallCredentials mockCredentials = mock(CallCredentials.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall);
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));

    AtomicLong time = new AtomicLong(0);
    FailoverChannel failoverChannel =
        createForTest(mockChannel, mockFallbackChannel, mockCredentials, time::get);

    triggerRPCFailure(failoverChannel, underlyingCall, Status.UNAVAILABLE);

    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);

    ArgumentCaptor<CallOptions> optionsCaptor = ArgumentCaptor.forClass(CallOptions.class);
    verify(mockFallbackChannel).newCall(same(methodDescriptor), optionsCaptor.capture());
    assertEquals(mockCredentials, optionsCaptor.getValue().getCredentials());
  }

  @Test
  public void testStateFallbackAfterPrimaryNotReady() {
    // If the state-change callback signals primary is not ready for 10+ seconds,
    // the next newCall() should route to fallback.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    when(mockChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    // IDLE for constructor registration, TRANSIENT_FAILURE when callback fires,
    // TRANSIENT_FAILURE for re-registration after the callback.
    when(mockChannel.getState(false))
        .thenReturn(
            ConnectivityState.IDLE,
            ConnectivityState.TRANSIENT_FAILURE,
            ConnectivityState.TRANSIENT_FAILURE);

    AtomicReference<Runnable> stateChangeCallback = new AtomicReference<>();
    doAnswer(
            invocation -> {
              stateChangeCallback.set(invocation.getArgument(1));
              return null;
            })
        .when(mockChannel)
        .notifyWhenStateChanged(any(), any());

    AtomicLong time = new AtomicLong(0);
    FailoverChannel failoverChannel =
        createForTest(mockChannel, mockFallbackChannel, null, time::get);

    // Callback fires: primary is TRANSIENT_FAILURE, starts the not-ready timer.
    stateChangeCallback.get().run();

    // Within 10 seconds: grace period not elapsed, routes to primary.
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockChannel).newCall(any(), any());

    // After 10 seconds: routes to fallback.
    time.addAndGet(TimeUnit.SECONDS.toNanos(11));
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockFallbackChannel).newCall(any(), any());
  }

  @Test
  public void testStateFallbackWhenPrimaryStartsNonReadyWithoutTransition() {
    // Primary starts in a non-ready state and stays there. Even without a state transition,
    // fallback should start after the 10s grace period.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    when(mockChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    when(mockChannel.getState(false)).thenReturn(ConnectivityState.TRANSIENT_FAILURE);

    AtomicLong time = new AtomicLong(0);
    FailoverChannel failoverChannel =
        createForTest(mockChannel, mockFallbackChannel, null, time::get);

    // Before grace period, still routes to primary.
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockChannel).newCall(any(), any());

    // After 10 seconds in non-ready state, should route to fallback.
    time.addAndGet(TimeUnit.SECONDS.toNanos(11));
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockFallbackChannel).newCall(any(), any());
  }

  @Test
  public void testIdleStateNotTreatedAsFallback() {
    // IDLE is a normal healthy state (channel is not actively connected but will reconnect on
    // demand). It must NOT start the not-ready timer or trigger state-based fallback, even after
    // more than 10 seconds.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    when(mockChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    // Primary stays IDLE the entire time (constructor registration + all state checks).
    when(mockChannel.getState(false)).thenReturn(ConnectivityState.IDLE);

    AtomicLong time = new AtomicLong(0);
    FailoverChannel failoverChannel =
        createForTest(mockChannel, mockFallbackChannel, null, time::get);

    // Advance well past the 10-second threshold while primary remains IDLE
    time.addAndGet(TimeUnit.SECONDS.toNanos(30));

    // IDLE must not trigger fallback — all calls still route to primary
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockChannel, atLeast(2)).newCall(any(), any());
    verify(mockFallbackChannel, never()).newCall(any(), any());
  }

  @Test
  public void testStateBasedFallbackRecoveryViaCallback() {
    // After state-based fallback, recovery to primary is immediate when callback fires with READY.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    when(mockChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    // getState() calls in order:
    // 1. constructor registerPrimaryStateChangeListener() → IDLE
    // 2. onPrimaryStateChanged() fires (TRANSIENT_FAILURE) → TRANSIENT_FAILURE
    // 3. re-registerPrimaryStateChangeListener() after 1st callback → TRANSIENT_FAILURE
    // 4. onPrimaryStateChanged() fires (READY) → READY
    // 5. re-registerPrimaryStateChangeListener() after 2nd callback → READY
    when(mockChannel.getState(false))
        .thenReturn(
            ConnectivityState.IDLE,
            ConnectivityState.TRANSIENT_FAILURE,
            ConnectivityState.TRANSIENT_FAILURE,
            ConnectivityState.READY,
            ConnectivityState.READY);

    AtomicReference<Runnable> stateChangeCallback = new AtomicReference<>();
    doAnswer(
            invocation -> {
              stateChangeCallback.set(invocation.getArgument(1));
              return null;
            })
        .when(mockChannel)
        .notifyWhenStateChanged(any(), any());

    AtomicLong time = new AtomicLong(0);
    FailoverChannel failoverChannel =
        createForTest(mockChannel, mockFallbackChannel, null, time::get);

    // First callback fires: primary is TRANSIENT_FAILURE, starts the not-ready timer at t=0.
    stateChangeCallback.get().run();

    // Within grace period: routes to primary.
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockChannel).newCall(any(), any());

    // After 10 seconds: state-based fallback kicks in.
    time.addAndGet(TimeUnit.SECONDS.toNanos(11));
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockFallbackChannel).newCall(any(), any());

    // Second callback fires: primary is now READY, clears all fallback state.
    stateChangeCallback.get().run();

    // Next call recovers to primary immediately.
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockChannel, atLeast(2)).newCall(any(), any());
  }

  // --- DEADLINE_EXCEEDED tests ---

  @Test
  public void testDeadlineExceededWithoutResponseTriggersFallback() throws Exception {
    // DEADLINE_EXCEEDED with no response = connection never established. Should failover.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall);
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));

    FailoverChannel failoverChannel = createForTest(mockChannel, mockFallbackChannel);

    ClientCall<Object, Object> call1 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    Metadata metadata1 = new Metadata();
    call1.start(new NoopClientCall.NoopClientCallListener<>(), metadata1);

    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(underlyingCall).start(captor.capture(), same(metadata1));
    // Close with DEADLINE_EXCEEDED and no prior onMessage, should trigger failover
    captor.getValue().onClose(Status.DEADLINE_EXCEEDED, new Metadata());

    ClientCall<Object, Object> call2 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call2.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    verify(mockFallbackChannel, atLeastOnce()).newCall(any(), any());
  }

  @Test
  public void testDeadlineExceededWithResponseDoesNotTriggerFallback() throws Exception {
    // DEADLINE_EXCEEDED after receiving a response, should NOT
    // failover. The connection was healthy since at least one response was delivered.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall, mock(ClientCall.class));
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));

    FailoverChannel failoverChannel = createForTest(mockChannel, mockFallbackChannel);

    ClientCall<Object, Object> call1 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    Metadata metadata1 = new Metadata();
    call1.start(new NoopClientCall.NoopClientCallListener<>(), metadata1);

    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(underlyingCall).start(captor.capture(), same(metadata1));
    // Simulate receiving a response before the timeout
    captor.getValue().onMessage(new Object());
    // Close with DEADLINE_EXCEEDED after a response, should NOT trigger failover
    captor.getValue().onClose(Status.DEADLINE_EXCEEDED, new Metadata());

    // Next call should still route to primary
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockChannel, atLeast(2)).newCall(any(), any());
    verify(mockFallbackChannel, never()).newCall(any(), any());
  }

  // --- Concurrency tests ---

  @Test
  public void testConcurrentRPCFailuresProduceConsistentFailover() throws Exception {
    // Concurrent RPC failures from multiple threads should produce exactly one failover.
    // After all threads complete, subsequent calls must consistently route to fallback.
    int numThreads = 20;
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);

    // Track all primary ClientCalls created so we can fire onClose on each
    List<ClientCall<Object, Object>> primaryCalls = Collections.synchronizedList(new ArrayList<>());
    when(mockChannel.newCall(any(), any()))
        .thenAnswer(
            inv -> {
              ClientCall<Object, Object> call = mock(ClientCall.class);
              primaryCalls.add(call);
              return call;
            });
    when(mockFallbackChannel.newCall(any(), any())).thenAnswer(inv -> mock(ClientCall.class));
    // Ensure state-based fallback does not interfere
    when(mockChannel.getState(false)).thenReturn(ConnectivityState.READY);

    FailoverChannel failoverChannel = createForTest(mockChannel, mockFallbackChannel);

    // Start N calls on primary and capture their listeners
    List<Listener<Object>> listeners = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      ClientCall<Object, Object> call =
          failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
      call.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    }
    for (ClientCall<Object, Object> primaryCall : primaryCalls) {
      ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
      verify(primaryCall).start(captor.capture(), any());
      listeners.add(captor.getValue());
    }

    // All threads fire UNAVAILABLE simultaneously
    CyclicBarrier barrier = new CyclicBarrier(numThreads);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      final Listener<Object> listener = listeners.get(i);
      Thread t =
          new Thread(
              () -> {
                try {
                  barrier.await();
                  listener.onClose(Status.UNAVAILABLE, new Metadata());
                } catch (Exception e) {
                  Thread.currentThread().interrupt();
                }
              });
      t.start();
      threads.add(t);
    }
    for (Thread t : threads) {
      t.join(5000);
    }

    // All subsequent calls must consistently route to fallback
    int subsequentCalls = 5;
    for (int i = 0; i < subsequentCalls; i++) {
      failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    }
    verify(mockFallbackChannel, atLeast(subsequentCalls)).newCall(any(), any());
  }

  @Test
  public void testConcurrentNewCallsDuringRPCFailoverAreConsistent() throws Exception {
    // Calls made concurrently while RPC failover is triggered must route consistently:
    // none should be lost and each must go to either primary (before failover) or
    // fallback (after).
    int numThreads = 20;
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);

    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall);
    when(mockFallbackChannel.newCall(any(), any())).thenAnswer(inv -> mock(ClientCall.class));
    when(mockChannel.getState(false)).thenReturn(ConnectivityState.READY);

    FailoverChannel failoverChannel = createForTest(mockChannel, mockFallbackChannel);

    // Set up an in-flight primary call whose failure will trigger RPC-based failover.
    ClientCall<Object, Object> triggerCall =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    triggerCall.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    ArgumentCaptor<Listener<Object>> listenerCaptor =
        ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(underlyingCall).start(listenerCaptor.capture(), any());
    ClientCall.Listener<Object> wrappedListener = listenerCaptor.getValue();

    // All threads (failover trigger + newCall callers) start simultaneously via a barrier.
    CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
    List<Callable<Void>> tasks = new ArrayList<>();
    tasks.add(
        () -> {
          barrier.await();
          wrappedListener.onClose(Status.UNAVAILABLE, new Metadata());
          return null;
        });
    for (int i = 0; i < numThreads; i++) {
      tasks.add(
          () -> {
            barrier.await();
            failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
            return null;
          });
    }

    ExecutorService executor = Executors.newFixedThreadPool(numThreads + 1);
    executor.invokeAll(tasks, 5, TimeUnit.SECONDS);
    executor.shutdown();

    // After concurrent operations, state must be coherent: subsequent calls go to fallback.
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockFallbackChannel, atLeastOnce()).newCall(any(), any());
  }
}
