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

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.fn.stream.AdvancingPhaser;
import org.apache.beam.sdk.fn.stream.BufferingStreamObserver;
import org.apache.beam.sdk.fn.stream.DirectStreamObserver;
import org.apache.beam.sdk.fn.stream.ForwardingClientResponseObserver;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Uses {@link PipelineOptions} to configure which underlying {@link StreamObserver} implementation
 * to use.
 */
public abstract class StreamObserverFactory {
  public static StreamObserverFactory fromOptions(PipelineOptions options) {
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    if (experiments != null && experiments.contains("beam_fn_api_buffered_stream")) {
      int bufferSize = Buffered.DEFAULT_BUFFER_SIZE;
      for (String experiment : experiments) {
        if (experiment.startsWith("beam_fn_api_buffered_stream_buffer_size=")) {
          bufferSize = Integer.parseInt(
              experiment.substring("beam_fn_api_buffered_stream_buffer_size=".length()));
        }
      }
      return new Buffered(options.as(GcsOptions.class).getExecutorService(), bufferSize);
    }
    return new Direct();
  }

  public abstract <ReqT, RespT> StreamObserver<RespT> from(
      Function<StreamObserver<ReqT>, StreamObserver<RespT>> clientFactory,
      StreamObserver<ReqT> responseObserver);

  private static class Direct extends StreamObserverFactory {

    @Override
    public <ReqT, RespT> StreamObserver<RespT> from(
        Function<StreamObserver<ReqT>, StreamObserver<RespT>> clientFactory,
        StreamObserver<ReqT> inboundObserver) {
      AdvancingPhaser phaser = new AdvancingPhaser(1);
      CallStreamObserver<RespT> outboundObserver =
          (CallStreamObserver<RespT>)
              clientFactory.apply(
                  ForwardingClientResponseObserver.<ReqT, RespT>create(
                      inboundObserver, phaser::arrive));
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
        Function<StreamObserver<ReqT>, StreamObserver<RespT>> clientFactory,
        StreamObserver<ReqT> inboundObserver) {
      AdvancingPhaser phaser = new AdvancingPhaser(1);
      CallStreamObserver<RespT> outboundObserver =
          (CallStreamObserver<RespT>)
              clientFactory.apply(
                  ForwardingClientResponseObserver.<ReqT, RespT>create(
                      inboundObserver, phaser::arrive));
      return new BufferingStreamObserver<>(
          phaser, outboundObserver, executorService, bufferSize);
    }

  }
}
