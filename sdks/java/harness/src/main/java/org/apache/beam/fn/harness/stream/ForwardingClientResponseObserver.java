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

package org.apache.beam.fn.harness.stream;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link ClientResponseObserver} which delegates all {@link StreamObserver} calls.
 *
 * <p>Used to wrap existing {@link StreamObserver}s to be able to install an
 * {@link ClientCallStreamObserver#setOnReadyHandler(Runnable) onReadyHandler}.
 */
@NotThreadSafe
final class ForwardingClientResponseObserver<ReqT, RespT>
    implements ClientResponseObserver<RespT, ReqT> {
  private final Runnable onReadyHandler;
  private final StreamObserver<ReqT> inboundObserver;

  ForwardingClientResponseObserver(
      StreamObserver<ReqT> inboundObserver, Runnable onReadyHandler) {
    this.inboundObserver = inboundObserver;
    this.onReadyHandler = onReadyHandler;
  }

  @Override
  public final void onNext(ReqT value) {
    inboundObserver.onNext(value);
  }

  @Override
  public final void onError(Throwable t) {
    inboundObserver.onError(t);
  }

  @Override
  public final void onCompleted() {
    inboundObserver.onCompleted();
  }

  @Override
  public final void beforeStart(ClientCallStreamObserver<RespT> stream) {
    stream.setOnReadyHandler(onReadyHandler);
  }
}