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

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutorService;

/**
 * Creates factories which determine an underlying {@link StreamObserver} implementation
 * to use in to interact with fn execution APIs.
 */
public abstract class StreamObserverFactory {
  /**
   * Create a buffering {@link StreamObserverFactory} with the specified {@link ExecutorService} and
   * the default buffer size.
   */
  public static StreamObserverFactory buffered(ExecutorService executorService) {
    return new Buffered(executorService, Buffered.DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a buffering {@link StreamObserverFactory} with the specified {@link ExecutorService} and
   * buffer size.
   */
  public static StreamObserverFactory buffered(
      ExecutorService executorService, int bufferSize) {
    return new Buffered(executorService, bufferSize);
  }

  /**
   * Create the default {@link StreamObserverFactory}.
   */
  public static StreamObserverFactory direct() {
    return new Direct();
  }

  public abstract <ReqT, RespT> StreamObserver<RespT> from(
      StreamObserverClientFactory<ReqT, RespT> clientFactory,
      StreamObserver<ReqT> responseObserver);

  private static class Direct extends StreamObserverFactory {

    @Override
    public <ReqT, RespT> StreamObserver<RespT> from(
        StreamObserverClientFactory<ReqT, RespT> clientFactory,
        StreamObserver<ReqT> inboundObserver) {
      AdvancingPhaser phaser = new AdvancingPhaser(1);
      CallStreamObserver<RespT> outboundObserver =
          (CallStreamObserver<RespT>)
              clientFactory.outboundObserverFor(
                  ForwardingClientResponseObserver.<ReqT, RespT>create(
                      inboundObserver, StreamObserverFactory.arriveAtPhaserHandler(phaser)));
      return new DirectStreamObserver<>(phaser, outboundObserver);
    }
  }

  private static class Buffered extends StreamObserverFactory {
    private static final int DEFAULT_BUFFER_SIZE = 64;
    private final ExecutorService executorService;
    private final int bufferSize;

    private Buffered(ExecutorService executorService, int bufferSize) {
      this.executorService = executorService;
      this.bufferSize = bufferSize;
    }

    @Override
    public <ReqT, RespT> StreamObserver<RespT> from(
        StreamObserverClientFactory<ReqT, RespT> clientFactory,
        StreamObserver<ReqT> inboundObserver) {
      AdvancingPhaser phaser = new AdvancingPhaser(1);
      CallStreamObserver<RespT> outboundObserver =
          (CallStreamObserver<RespT>)
              clientFactory.outboundObserverFor(
                  ForwardingClientResponseObserver.<ReqT, RespT>create(
                      inboundObserver, StreamObserverFactory.arriveAtPhaserHandler(phaser)));
      return new BufferingStreamObserver<>(
          phaser, outboundObserver, executorService, bufferSize);
    }
  }

  private static Runnable arriveAtPhaserHandler(final AdvancingPhaser phaser) {
    return phaser::arrive;
  }

  /**
   * A factory which creates {@link StreamObserver StreamObservers} based on the input stream
   * observer.
   *
   * <p>For example, this could be used to
   *
   * @param <RequestT> The type of message sent to the inbound stream
   * @param <ResponseT> They type of messages sent over the outbound stream
   */
  public interface StreamObserverClientFactory<RequestT, ResponseT> {
    StreamObserver<ResponseT> outboundObserverFor(StreamObserver<RequestT> inboundObserver);
  }
}
