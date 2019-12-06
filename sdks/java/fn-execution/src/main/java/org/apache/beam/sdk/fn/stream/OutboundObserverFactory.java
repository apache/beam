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

import java.util.concurrent.ExecutorService;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.StreamObserver;

/**
 * Creates factories which determine an underlying {@link StreamObserver} implementation to use in
 * to interact with fn execution APIs.
 */
public abstract class OutboundObserverFactory {
  /**
   * Create a buffering {@link OutboundObserverFactory} for client-side RPCs with the specified
   * {@link ExecutorService} and the default buffer size. All {@link StreamObserver}s created by
   * this factory are thread safe.
   */
  public static OutboundObserverFactory clientBuffered(ExecutorService executorService) {
    return new Buffered(executorService, Buffered.DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a buffering {@link OutboundObserverFactory} for client-side RPCs with the specified
   * {@link ExecutorService} and buffer size. All {@link StreamObserver}s created by this factory
   * are thread safe.
   */
  public static OutboundObserverFactory clientBuffered(
      ExecutorService executorService, int bufferSize) {
    return new Buffered(executorService, bufferSize);
  }

  /**
   * Create the default {@link OutboundObserverFactory} for client-side RPCs, which uses basic
   * unbuffered flow control. All {@link StreamObserver}s created by this factory are thread safe.
   */
  public static OutboundObserverFactory clientDirect() {
    return new DirectClient();
  }

  /** Like {@link #clientDirect} but for server-side RPCs. */
  public static OutboundObserverFactory serverDirect() {
    return new DirectServer();
  }

  /**
   * Creates an {@link OutboundObserverFactory} that simply delegates to the base factory, with no
   * flow control or synchronization. Not recommended for use except in tests.
   */
  public static OutboundObserverFactory trivial() {
    return new Trivial();
  }

  /** Creates an outbound observer for the given inbound observer. */
  @FunctionalInterface
  public interface BasicFactory<ReqT, RespT> {
    StreamObserver<RespT> outboundObserverFor(StreamObserver<ReqT> inboundObserver);
  }

  /**
   * Creates an outbound observer for the given inbound observer by potentially inserting hooks into
   * the inbound and outbound observers.
   *
   * @param baseOutboundObserverFactory A base function to create an outbound observer from an
   *     inbound observer.
   * @param inboundObserver The inbound observer.
   */
  public abstract <ReqT, RespT> StreamObserver<RespT> outboundObserverFor(
      BasicFactory<ReqT, RespT> baseOutboundObserverFactory, StreamObserver<ReqT> inboundObserver);

  private static class DirectClient extends OutboundObserverFactory {
    @Override
    public <ReqT, RespT> StreamObserver<RespT> outboundObserverFor(
        BasicFactory<ReqT, RespT> baseOutboundObserverFactory,
        StreamObserver<ReqT> inboundObserver) {
      AdvancingPhaser phaser = new AdvancingPhaser(1);
      inboundObserver = ForwardingClientResponseObserver.create(inboundObserver, phaser::arrive);
      CallStreamObserver<RespT> outboundObserver =
          (CallStreamObserver<RespT>)
              baseOutboundObserverFactory.outboundObserverFor(inboundObserver);
      return new DirectStreamObserver<>(phaser, outboundObserver);
    }
  }

  private static class DirectServer extends OutboundObserverFactory {
    @Override
    public <ReqT, RespT> StreamObserver<RespT> outboundObserverFor(
        BasicFactory<ReqT, RespT> baseOutboundObserverFactory,
        StreamObserver<ReqT> inboundObserver) {
      AdvancingPhaser phaser = new AdvancingPhaser(1);
      CallStreamObserver<RespT> outboundObserver =
          (CallStreamObserver<RespT>)
              baseOutboundObserverFactory.outboundObserverFor(inboundObserver);
      outboundObserver.setOnReadyHandler(phaser::arrive);
      return new DirectStreamObserver<>(phaser, outboundObserver);
    }
  }

  private static class Buffered extends OutboundObserverFactory {
    private static final int DEFAULT_BUFFER_SIZE = 64;
    private final ExecutorService executorService;
    private final int bufferSize;

    private Buffered(ExecutorService executorService, int bufferSize) {
      this.executorService = executorService;
      this.bufferSize = bufferSize;
    }

    @Override
    public <ReqT, RespT> StreamObserver<RespT> outboundObserverFor(
        BasicFactory<ReqT, RespT> baseOutboundObserverFactory,
        StreamObserver<ReqT> inboundObserver) {
      AdvancingPhaser phaser = new AdvancingPhaser(1);
      inboundObserver = ForwardingClientResponseObserver.create(inboundObserver, phaser::arrive);
      CallStreamObserver<RespT> outboundObserver =
          (CallStreamObserver<RespT>)
              baseOutboundObserverFactory.outboundObserverFor(inboundObserver);
      return new BufferingStreamObserver<>(phaser, outboundObserver, executorService, bufferSize);
    }
  }

  private static class Trivial extends OutboundObserverFactory {
    @Override
    public <ReqT, RespT> StreamObserver<RespT> outboundObserverFor(
        BasicFactory<ReqT, RespT> baseOutboundObserverFactory,
        StreamObserver<ReqT> inboundObserver) {
      return baseOutboundObserverFactory.outboundObserverFor(inboundObserver);
    }
  }
}
