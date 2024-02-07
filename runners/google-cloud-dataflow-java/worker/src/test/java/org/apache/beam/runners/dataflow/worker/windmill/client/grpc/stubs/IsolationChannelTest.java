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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientCall.Listener;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.MethodDescriptor.Marshaller;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.internal.NoopClientCall;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

@RunWith(JUnit4.class)
public class IsolationChannelTest {

  private Supplier<ManagedChannel> channelSupplier = mock(Supplier.class);

  private static class NoopMarshaller implements Marshaller<Object> {

    @Override
    public InputStream stream(Object o) {
      return new InputStream() {
        @Override
        public int read() throws IOException {
          return 0;
        }
      };
    }

    @Override
    public Object parse(InputStream inputStream) {
      return null;
    }
  };

  private MethodDescriptor<Object, Object> methodDescriptor =
      MethodDescriptor.newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(MethodDescriptor.generateFullMethodName("test", "test"))
          .setRequestMarshaller(new NoopMarshaller())
          .setResponseMarshaller(new NoopMarshaller())
          .build();

  @Test
  public void singleChannelAuthority() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(mockChannel.authority()).thenReturn("TEST_AUTHORITY");
    when(channelSupplier.get()).thenReturn(mockChannel);

    IsolationChannel isolationChannel = IsolationChannel.create(channelSupplier);
    assertTrue(isolationChannel.authority().equals("TEST_AUTHORITY"));
    verify(channelSupplier, times(1)).get();
  }

  @Test
  public void singleChannelSuccessReuse() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(channelSupplier.get()).thenReturn(mockChannel);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall);

    IsolationChannel isolationChannel = IsolationChannel.create(channelSupplier);
    ClientCall<Object, Object> call1 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);

    Metadata metadata1 = new Metadata();
    call1.start(new NoopClientCall.NoopClientCallListener<>(), metadata1);

    InOrder inOrder = inOrder(underlyingCall);

    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    inOrder.verify(underlyingCall).start(captor.capture(), same(metadata1));
    captor.getValue().onClose(Status.OK, new Metadata());

    ClientCall<Object, Object> call2 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);

    Metadata metadata2 = new Metadata();
    call2.start(new NoopClientCall.NoopClientCallListener<>(), metadata2);
    captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    inOrder.verify(underlyingCall).start(captor.capture(), same(metadata2));
    captor.getValue().onClose(Status.OK, new Metadata());

    verify(channelSupplier, times(1)).get();
    verify(mockChannel, times(2)).newCall(any(), any());
  }

  @Test
  public void singleChannelErrorReused() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(channelSupplier.get()).thenReturn(mockChannel);
    ClientCall<Object, Object> underlyingCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(underlyingCall);

    IsolationChannel isolationChannel = IsolationChannel.create(channelSupplier);
    ClientCall<Object, Object> call1 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);

    Metadata metadata1 = new Metadata();
    call1.start(new NoopClientCall.NoopClientCallListener<>(), metadata1);

    InOrder inOrder = inOrder(underlyingCall);

    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    inOrder.verify(underlyingCall).start(captor.capture(), same(metadata1));
    captor.getValue().onClose(Status.OK, new Metadata());

    ClientCall<Object, Object> call2 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);

    Metadata metadata2 = new Metadata();
    call2.start(new NoopClientCall.NoopClientCallListener<>(), metadata2);
    captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    inOrder.verify(underlyingCall).start(captor.capture(), same(metadata2));
    captor.getValue().onClose(Status.ABORTED, new Metadata());

    verify(channelSupplier, times(1)).get();
    verify(mockChannel, times(2)).newCall(any(), any());
  }

  @Test
  public void singleChannelStartExceptionReused() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(channelSupplier.get()).thenReturn(mockChannel);
    ClientCall<Object, Object> exceptionCall = mock(ClientCall.class);
    ClientCall<Object, Object> successCall = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(exceptionCall, successCall);
    doThrow(new IllegalStateException("Test start error")).when(exceptionCall).start(any(), any());

    IsolationChannel isolationChannel = IsolationChannel.create(channelSupplier);
    ClientCall<Object, Object> call1 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);

    Metadata metadata1 = new Metadata();
    assertThrows(
        IllegalStateException.class,
        () -> call1.start(new NoopClientCall.NoopClientCallListener<>(), metadata1));

    ClientCall<Object, Object> call2 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);

    call2.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    ArgumentCaptor<Listener<Object>> captor = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(successCall).start(captor.capture(), any());
    captor.getValue().onClose(Status.ABORTED, new Metadata());

    verify(channelSupplier, times(1)).get();
    verify(mockChannel, times(2)).newCall(any(), any());
  }

  @Test
  public void multipleChannelsCreated() throws Exception {
    ManagedChannel mockChannel1 = mock(ManagedChannel.class);
    ManagedChannel mockChannel2 = mock(ManagedChannel.class);
    ManagedChannel mockChannel3 = mock(ManagedChannel.class);
    when(channelSupplier.get()).thenReturn(mockChannel1, mockChannel2, mockChannel3);
    ClientCall<Object, Object> mockCall1 = mock(ClientCall.class);
    when(mockChannel1.newCall(any(), any())).thenReturn(mockCall1);
    ClientCall<Object, Object> mockCall2 = mock(ClientCall.class);
    when(mockChannel2.newCall(any(), any())).thenReturn(mockCall2);
    ClientCall<Object, Object> mockCall3 = mock(ClientCall.class);
    when(mockChannel3.newCall(any(), any())).thenReturn(mockCall3);

    IsolationChannel isolationChannel = IsolationChannel.create(channelSupplier);

    ClientCall<Object, Object> call1 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call1.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());

    ArgumentCaptor<Listener<Object>> captor1 = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockCall1).start(captor1.capture(), any());

    ClientCall<Object, Object> call2 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call2.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());

    ArgumentCaptor<Listener<Object>> captor2 = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockCall2).start(captor2.capture(), any());

    ClientCall<Object, Object> call3 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call3.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());

    ArgumentCaptor<Listener<Object>> captor3 = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockCall3).start(captor3.capture(), any());

    captor1.getValue().onClose(Status.OK, new Metadata());
    captor2.getValue().onClose(Status.OK, new Metadata());
    captor3.getValue().onClose(Status.OK, new Metadata());

    verify(channelSupplier, times(3)).get();
    verify(mockChannel1, times(1)).newCall(any(), any());
    verify(mockChannel2, times(1)).newCall(any(), any());
    verify(mockChannel3, times(1)).newCall(any(), any());
  }

  @Test
  public void interleavedReuse() throws Exception {
    ManagedChannel mockChannel1 = mock(ManagedChannel.class);
    ManagedChannel mockChannel2 = mock(ManagedChannel.class);
    when(channelSupplier.get()).thenReturn(mockChannel1, mockChannel2);
    ClientCall<Object, Object> mockCall1 = mock(ClientCall.class);
    when(mockChannel1.newCall(any(), any())).thenReturn(mockCall1);
    ClientCall<Object, Object> mockCall2 = mock(ClientCall.class);
    ClientCall<Object, Object> mockCall3 = mock(ClientCall.class);
    when(mockChannel2.newCall(any(), any())).thenReturn(mockCall2, mockCall3);

    IsolationChannel isolationChannel = IsolationChannel.create(channelSupplier);

    ClientCall<Object, Object> call1 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call1.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());

    ArgumentCaptor<Listener<Object>> captor1 = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockCall1).start(captor1.capture(), any());

    ClientCall<Object, Object> call2 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call2.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());

    ArgumentCaptor<Listener<Object>> captor2 = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockCall2).start(captor2.capture(), any());

    // Finish call2, freeing up the channel
    captor2.getValue().onClose(Status.OK, new Metadata());

    ClientCall<Object, Object> call3 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call3.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());

    ArgumentCaptor<Listener<Object>> captor3 = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockCall3).start(captor3.capture(), any());

    captor1.getValue().onClose(Status.OK, new Metadata());
    captor3.getValue().onClose(Status.OK, new Metadata());

    verify(channelSupplier, times(2)).get();
    verify(mockChannel1, times(1)).newCall(any(), any());
    verify(mockChannel2, times(2)).newCall(any(), any());
  }

  @Test
  public void testShutdown() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(channelSupplier.get()).thenReturn(mockChannel);
    ClientCall<Object, Object> mockCall1 = mock(ClientCall.class);
    ClientCall<Object, Object> mockCall2 = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(mockCall1, mockCall2);

    IsolationChannel isolationChannel = IsolationChannel.create(channelSupplier);
    ClientCall<Object, Object> call1 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call1.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    ArgumentCaptor<Listener<Object>> captor1 = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockCall1).start(captor1.capture(), any());

    when(mockChannel.shutdown()).thenReturn(mockChannel);
    when(mockChannel.isShutdown()).thenReturn(false, true);

    isolationChannel.shutdown();
    assertFalse(isolationChannel.isShutdown());

    ClientCall<Object, Object> call2 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call2.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    ArgumentCaptor<Listener<Object>> captor2 = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockCall2).start(captor2.capture(), any());

    captor1.getValue().onClose(Status.CANCELLED, new Metadata());
    captor2.getValue().onClose(Status.CANCELLED, new Metadata());

    assertTrue(isolationChannel.isShutdown());

    verify(channelSupplier, times(1)).get();
    verify(mockChannel, times(2)).newCall(any(), any());
    verify(mockChannel, times(1)).shutdown();
    verify(mockChannel, times(2)).isShutdown();
  }

  @Test
  public void testShutdownNow() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(channelSupplier.get()).thenReturn(mockChannel);
    ClientCall<Object, Object> mockCall1 = mock(ClientCall.class);
    ClientCall<Object, Object> mockCall2 = mock(ClientCall.class);
    when(mockChannel.newCall(any(), any())).thenReturn(mockCall1, mockCall2);

    IsolationChannel isolationChannel = IsolationChannel.create(channelSupplier);
    ClientCall<Object, Object> call1 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call1.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    ArgumentCaptor<Listener<Object>> captor1 = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockCall1).start(captor1.capture(), any());

    when(mockChannel.shutdownNow()).thenReturn(mockChannel);
    when(mockChannel.isShutdown()).thenReturn(false, true);

    isolationChannel.shutdownNow();
    assertFalse(isolationChannel.isShutdown());

    ClientCall<Object, Object> call2 =
        isolationChannel.newCall(methodDescriptor, CallOptions.DEFAULT);
    call2.start(new NoopClientCall.NoopClientCallListener<>(), new Metadata());
    ArgumentCaptor<Listener<Object>> captor2 = ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(mockCall2).start(captor2.capture(), any());

    captor1.getValue().onClose(Status.CANCELLED, new Metadata());
    captor2.getValue().onClose(Status.CANCELLED, new Metadata());

    assertTrue(isolationChannel.isShutdown());

    verify(channelSupplier, times(1)).get();
    verify(mockChannel, times(2)).newCall(any(), any());
    verify(mockChannel, times(1)).shutdownNow();
    verify(mockChannel, times(2)).isShutdown();
  }

  @Test
  public void testIsTerminated() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(channelSupplier.get()).thenReturn(mockChannel);

    IsolationChannel isolationChannel = IsolationChannel.create(channelSupplier);

    when(mockChannel.shutdown()).thenReturn(mockChannel);
    when(mockChannel.isTerminated()).thenReturn(false, true);

    isolationChannel.shutdown();
    assertFalse(isolationChannel.isTerminated());
    assertTrue(isolationChannel.isTerminated());

    verify(channelSupplier, times(1)).get();
    verify(mockChannel, times(1)).shutdown();
    verify(mockChannel, times(2)).isTerminated();
  }

  @Test
  public void testAwaitTermination() throws Exception {
    ManagedChannel mockChannel = mock(ManagedChannel.class);
    when(channelSupplier.get()).thenReturn(mockChannel);

    IsolationChannel isolationChannel = IsolationChannel.create(channelSupplier);

    assertFalse(isolationChannel.awaitTermination(1, TimeUnit.MILLISECONDS));

    when(mockChannel.shutdown()).thenReturn(mockChannel);
    when(mockChannel.isTerminated()).thenReturn(false, false, false, true, true);
    when(mockChannel.awaitTermination(longThat(l -> l < 2_000_000), eq(TimeUnit.NANOSECONDS)))
        .thenReturn(false, true);

    isolationChannel.shutdown();
    assertFalse(isolationChannel.awaitTermination(1, TimeUnit.MILLISECONDS));
    assertTrue(isolationChannel.awaitTermination(1, TimeUnit.MILLISECONDS));

    assertTrue(isolationChannel.isTerminated());

    verify(channelSupplier, times(1)).get();
    verify(mockChannel, times(1)).shutdown();
    verify(mockChannel, times(5)).isTerminated();
    verify(mockChannel, times(2))
        .awaitTermination(longThat(l -> l < 2_000_000), eq(TimeUnit.NANOSECONDS));
  }
}
