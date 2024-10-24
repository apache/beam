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
package org.apache.beam.sdk.fn.stream;

import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.ClientCallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.ClientResponseObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;

/**
 * A {@link ClientResponseObserver} which delegates all {@link StreamObserver} calls.
 *
 * <p>Used to wrap existing {@link StreamObserver}s to be able to install an {@link
 * ClientCallStreamObserver#setOnReadyHandler(Runnable) onReadyHandler} and a handler invoked when
 * the stream terminates.
 *
 * <p>This is as thread-safe as the underlying stream observer that is being wrapped.
 */
public final class ForwardingClientResponseObserver<ReqT, RespT>
    implements ClientResponseObserver<RespT, ReqT> {
  public static <ReqT, RespT> ForwardingClientResponseObserver<ReqT, RespT> create(
      StreamObserver<ReqT> inbound, Runnable onReadyHandler) {
    return new ForwardingClientResponseObserver<>(inbound, onReadyHandler, () -> {});
  }

  public static <ReqT, RespT> ForwardingClientResponseObserver<ReqT, RespT> create(
      StreamObserver<ReqT> inbound, Runnable onReadyHandler, Runnable onDoneHandler) {
    return new ForwardingClientResponseObserver<>(inbound, onReadyHandler, onDoneHandler);
  }

  private final Runnable onReadyHandler;
  private final Runnable onDoneHandler;
  private final StreamObserver<ReqT> inboundObserver;

  private ForwardingClientResponseObserver(
      StreamObserver<ReqT> inboundObserver, Runnable onReadyHandler, Runnable onDoneHandler) {
    this.inboundObserver = inboundObserver;
    this.onReadyHandler = onReadyHandler;
    this.onDoneHandler = onDoneHandler;
  }

  @Override
  public void onNext(ReqT value) {
    inboundObserver.onNext(value);
  }

  @Override
  public void onError(Throwable t) {
    onDoneHandler.run();
    inboundObserver.onError(t);
  }

  @Override
  public void onCompleted() {
    onDoneHandler.run();
    inboundObserver.onCompleted();
  }

  @Override
  public void beforeStart(ClientCallStreamObserver<RespT> stream) {
    stream.setOnReadyHandler(onReadyHandler);
  }
}
