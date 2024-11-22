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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers;

import java.util.function.Function;
import org.apache.beam.sdk.fn.stream.AdvancingPhaser;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;

/**
 * Uses {@link PipelineOptions} to configure which underlying {@link StreamObserver} implementation
 * to use.
 */
public abstract class StreamObserverFactory {
  public static StreamObserverFactory direct(
      long deadlineSeconds, int messagesBetweenIsReadyChecks) {
    return new Direct(deadlineSeconds, messagesBetweenIsReadyChecks);
  }

  public abstract <ResponseT, RequestT> TerminatingStreamObserver<RequestT> from(
      Function<StreamObserver<ResponseT>, StreamObserver<RequestT>> clientFactory,
      StreamObserver<ResponseT> responseObserver);

  private static class Direct extends StreamObserverFactory {
    private final long deadlineSeconds;
    private final int messagesBetweenIsReadyChecks;

    Direct(long deadlineSeconds, int messagesBetweenIsReadyChecks) {
      this.deadlineSeconds = deadlineSeconds;
      this.messagesBetweenIsReadyChecks = messagesBetweenIsReadyChecks;
    }

    @Override
    public <ResponseT, RequestT> TerminatingStreamObserver<RequestT> from(
        Function<StreamObserver<ResponseT>, StreamObserver<RequestT>> clientFactory,
        StreamObserver<ResponseT> inboundObserver) {
      AdvancingPhaser phaser = new AdvancingPhaser(1);
      CallStreamObserver<RequestT> outboundObserver =
          (CallStreamObserver<RequestT>)
              clientFactory.apply(
                  new ForwardingClientResponseObserver<ResponseT, RequestT>(
                      inboundObserver, phaser::arrive, phaser::forceTermination));
      return new DirectStreamObserver<>(
          phaser, outboundObserver, deadlineSeconds, messagesBetweenIsReadyChecks);
    }
  }
}
