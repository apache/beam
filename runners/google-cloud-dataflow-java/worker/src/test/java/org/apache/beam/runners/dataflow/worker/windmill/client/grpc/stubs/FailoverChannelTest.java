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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.CallCredentials;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ClientCall.Listener;
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
  public void testFallbackAndRetry() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    ClientCall<Object, Object> fallbackCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall);
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(fallbackCall);

    FailoverChannel failoverChannel = FailoverChannel.create(mockChannel, mockFallbackChannel);

    // First call, triggers fallback
    ClientCall<Object, Object> call1 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    Metadata metadata1 = new Metadata();
    call1.start(new NoopClientCall.NoopClientCallListener<>(), metadata1);

    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(underlyingCall).start(captor.capture(), same(metadata1));

    // Fail with UNAVAILABLE
    captor.getValue().onClose(Status.UNAVAILABLE, new Metadata());

    // Second call should use fallback Channel
    ClientCall<Object, Object> call2 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call2.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    verify(mockFallbackChannel, atLeastOnce()).newCall(any(), any());
  }

  @Test
  public void testFallbackAndPeriodicRetry() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    ClientCall<Object, Object> underlyingCall2 = mock(ClientCall.class);
    ClientCall<Object, Object> fallbackCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall, underlyingCall2);
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(fallbackCall);

    AtomicLong time = new AtomicLong(0);

    FailoverChannel failoverChannel =
        FailoverChannel.forTest(mockChannel, mockFallbackChannel, null, time::get);

    // Trigger fallback
    ClientCall<Object, Object> call1 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call1.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(underlyingCall).start(captor.capture(), any());
    captor.getValue().onClose(Status.UNAVAILABLE, new Metadata());

    // Advance time by 30 mins (less than 1 hour)
    time.addAndGet(TimeUnit.MINUTES.toNanos(30));

    // Should still use fallback
    ClientCall<Object, Object> call2 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call2.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    verify(mockFallbackChannel, atLeastOnce()).newCall(any(), any());

    // Advance time by another 40 mins (total > 1 hour)
    time.addAndGet(TimeUnit.MINUTES.toNanos(40));

    // Should retry direct path
    ClientCall<Object, Object> call3 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call3.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    verify(mockChannel, atLeast(2)).newCall(any(), any());
  }

  @Test
  public void testFallbackWithCredentials() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    ManagedChannel mockFallbackChannel = mock(ManagedChannel.class);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    ClientCall<Object, Object> fallbackCall = mock(ClientCall.class);
    CallCredentials mockCredentials = mock(CallCredentials.class);

    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall);
    when(mockFallbackChannel.newCall(any(), any())).thenReturn(fallbackCall);

    FailoverChannel failoverChannel =
        FailoverChannel.create(mockChannel, mockFallbackChannel, mockCredentials);

    // Trigger fallback
    ClientCall<Object, Object> call1 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call1.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(underlyingCall).start(captor.capture(), any());
    captor.getValue().onClose(Status.UNAVAILABLE, new Metadata());

    // Next call should use fallback with credentials
    ClientCall<Object, Object> call2 =
        failoverChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call2.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());

    ArgumentCaptor<CallOptions> optionsCaptor = ArgumentCaptor.forClass(CallOptions.class);
    verify(mockFallbackChannel).newCall(same(methodDescriptor), optionsCaptor.capture());
    assertEquals(mockCredentials, optionsCaptor.getValue().getCredentials());
  }
}
