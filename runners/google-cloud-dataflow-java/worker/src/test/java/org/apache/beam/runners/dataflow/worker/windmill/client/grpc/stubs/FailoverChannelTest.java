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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
          .setRequestMarshaller(new IsolationChannelTest.NoopMarshaller())
          .setResponseMarshaller(new IsolationChannelTest.NoopMarshaller())
          .build();

  @Test
  public void testRPCFailureTriggersFallback() throws Exception {
    // RPC failure with UNAVAILABLE should switch to fallback channel.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    ClientCall<Object, Object> fallbackCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall);
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(fallbackCall);

    FailoverChannel failoverChannel = FailoverChannel.create(mockChannel, mockFallbackChannel);

    ClientCall<Object, Object> call1 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    Metadata metadata1 = new Metadata();
    call1.start(new NoopClientCall.NoopClientCallListener<>(), metadata1);

    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(underlyingCall).start(captor.capture(), same(metadata1));
    captor.getValue().onClose(Status.UNAVAILABLE, new Metadata());

    ClientCall<Object, Object> call2 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call2.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    verify(mockFallbackChannel, atLeastOnce()).newCall(any(), any());
  }

  @Test
  public void testRPCFailureRecoveryAfterCoolingPeriod() throws Exception {
    // After RPC failure, channel stays on fallback during cooling period, then returns to primary.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    ClientCall<Object, Object> fallbackCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall, mock(ClientCall.class));
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(fallbackCall);
    when(mockChannel.getState(true)).thenReturn(ConnectivityState.READY);

    AtomicLong time = new AtomicLong(0);
    FailoverChannel failoverChannel =
        FailoverChannel.forTest(mockChannel, mockFallbackChannel, null, time::get);

    // Trigger RPC failure fallback
    ClientCall<Object, Object> call1 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call1.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(underlyingCall).start(captor.capture(), any());
    captor.getValue().onClose(Status.UNAVAILABLE, new Metadata());

    // Within cooling period: still on fallback
    time.addAndGet(TimeUnit.MINUTES.toNanos(30));
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockFallbackChannel, atLeastOnce()).newCall(any(), any());

    // After cooling period: recovers to primary
    time.addAndGet(TimeUnit.MINUTES.toNanos(40));
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

    FailoverChannel failoverChannel =
        FailoverChannel.create(mockChannel, mockFallbackChannel, mockCredentials);

    ClientCall<Object, Object> call1 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call1.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(underlyingCall).start(captor.capture(), any());
    captor.getValue().onClose(Status.UNAVAILABLE, new Metadata());

    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);

    ArgumentCaptor<CallOptions> optionsCaptor = ArgumentCaptor.forClass(CallOptions.class);
    verify(mockFallbackChannel).newCall(same(methodDescriptor), optionsCaptor.capture());
    assertEquals(mockCredentials, optionsCaptor.getValue().getCredentials());
  }

  @Test
  public void testStateFallbackAfterPrimaryNotReady() {
    // If primary connection is not ready for 10+ seconds, routes to fallback.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    when(mockChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    when(mockChannel.getState(true)).thenReturn(ConnectivityState.IDLE, ConnectivityState.IDLE);

    AtomicLong time = new AtomicLong(0);
    FailoverChannel failoverChannel =
        FailoverChannel.forTest(mockChannel, mockFallbackChannel, null, time::get);

    // Within 10 seconds: still routes to primary
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockChannel).newCall(any(), any());

    // After 10 seconds: routes to fallback
    time.addAndGet(TimeUnit.SECONDS.toNanos(11));
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockFallbackChannel).newCall(any(), any());
  }

  @Test
  public void testStateBasedFallbackRecoveryViaCallback() {
    // After state-based fallback, recovery to primary is immediate when callback fires with READY.
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    when(mockChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(mock(ClientCall.class));
    // getState(true): IDLE starts timer, IDLE exceeds timer, READY on recovery check
    when(mockChannel.getState(true))
        .thenReturn(ConnectivityState.IDLE, ConnectivityState.IDLE, ConnectivityState.READY);
    // getState(false): IDLE for constructor registration, READY when callback fires
    when(mockChannel.getState(false))
        .thenReturn(ConnectivityState.IDLE, ConnectivityState.READY, ConnectivityState.READY);

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
        FailoverChannel.forTest(mockChannel, mockFallbackChannel, null, time::get);

    // First call - primary not yet timed out, routes to primary
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockChannel).newCall(any(), any());

    // After 10 seconds: state-based fallback kicks in
    time.addAndGet(TimeUnit.SECONDS.toNanos(11));
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockFallbackChannel).newCall(any(), any());

    // Callback fires with primary now READY: clears state flag immediately
    stateChangeCallback.get().run();

    // Next call recovers to primary with no waiting
    failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    verify(mockChannel, atLeast(2)).newCall(any(), any());
  }
}
