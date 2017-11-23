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

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

/**
 * A {@link ClientResponseObserver} which delegates all {@link StreamObserver} calls.
 *
 * <p>Used to wrap existing {@link StreamObserver}s to be able to install an
 * {@link ClientCallStreamObserver#setOnReadyHandler(Runnable) onReadyHandler}.
 *
 * <p>This is as thread-safe as the underlying stream observer that is being wrapped.
 */
public final class ForwardingClientResponseObserver<ReqT, RespT>
    implements ClientResponseObserver<RespT, ReqT> {
  public static <ReqT, RespT> ForwardingClientResponseObserver<ReqT, RespT> create(
      StreamObserver<ReqT> inbound, Runnable onReadyHandler) {
    return new ForwardingClientResponseObserver<>(inbound, onReadyHandler);
  }

  private final Runnable onReadyHandler;
  private final StreamObserver<ReqT> inboundObserver;

  ForwardingClientResponseObserver(
      StreamObserver<ReqT> inboundObserver, Runnable onReadyHandler) {
    this.inboundObserver = inboundObserver;
    this.onReadyHandler = onReadyHandler;
  }

  @Override
  public void onNext(ReqT value) {
    inboundObserver.onNext(value);
  }

  @Override
  public void onError(Throwable t) {
    inboundObserver.onError(t);
  }

  @Override
  public void onCompleted() {
    inboundObserver.onCompleted();
  }

  @Override
  public void beforeStart(ClientCallStreamObserver<RespT> stream) {
    stream.setOnReadyHandler(onReadyHandler);
  }
}
