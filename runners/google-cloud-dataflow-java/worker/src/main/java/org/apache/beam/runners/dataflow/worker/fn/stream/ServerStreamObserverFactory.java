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
package org.apache.beam.runners.dataflow.worker.fn.stream;

import java.util.concurrent.ExecutorService;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.fn.stream.AdvancingPhaser;
import org.apache.beam.sdk.fn.stream.BufferingStreamObserver;
import org.apache.beam.sdk.fn.stream.DirectStreamObserver;
import org.apache.beam.sdk.fn.stream.ForwardingClientResponseObserver;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.ServerCallStreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;

/**
 * A {@link StreamObserver} factory that wraps provided {@link CallStreamObserver}s making them flow
 * control aware. This is different than the {@link OutboundObserverFactory} since {@link
 * ServerCallStreamObserver}s can have their {@link CallStreamObserver#setOnReadyHandler} set
 * directly. This allows us to avoid needing to use a {@link ForwardingClientResponseObserver} to
 * wrap the inbound {@link StreamObserver}.
 *
 * <p>The specific implementation returned is dependent on {@link PipelineOptions} experiments.
 */
public abstract class ServerStreamObserverFactory {
  public static ServerStreamObserverFactory fromOptions(PipelineOptions options) {
    DataflowPipelineDebugOptions dataflowOptions = options.as(DataflowPipelineDebugOptions.class);
    if (DataflowRunner.hasExperiment(dataflowOptions, "beam_fn_api_buffered_stream")) {
      int bufferSize = Buffered.DEFAULT_BUFFER_SIZE;
      for (String experiment : dataflowOptions.getExperiments()) {
        if (experiment.startsWith("beam_fn_api_buffered_stream_buffer_size=")) {
          bufferSize =
              Integer.parseInt(
                  experiment.substring("beam_fn_api_buffered_stream_buffer_size=".length()));
        }
      }
      return new Buffered(options.as(GcsOptions.class).getExecutorService(), bufferSize);
    }
    return new Direct();
  }

  public abstract <T> StreamObserver<T> from(StreamObserver<T> outboundObserver);

  /**
   * Wraps provided outbound {@link StreamObserver}s with flow control aware {@link
   * DirectStreamObserver}s.
   */
  private static class Direct extends ServerStreamObserverFactory {

    @Override
    public <T> StreamObserver<T> from(StreamObserver<T> outboundObserver) {
      AdvancingPhaser phaser = new AdvancingPhaser(1);
      CallStreamObserver<T> outboundCallStreamObserver = (CallStreamObserver<T>) outboundObserver;
      outboundCallStreamObserver.setOnReadyHandler(phaser::arrive);
      return new DirectStreamObserver<>(phaser, outboundCallStreamObserver);
    }
  }

  /**
   * Wraps provided outbound {@link StreamObserver}s with flow control aware {@link
   * BufferingStreamObserver}s.
   */
  private static class Buffered extends ServerStreamObserverFactory {
    private static final int DEFAULT_BUFFER_SIZE = 64;
    private final ExecutorService executorService;
    private final int bufferSize;

    private Buffered(ExecutorService executorService, int bufferSize) {
      this.executorService = executorService;
      this.bufferSize = bufferSize;
    }

    @Override
    public <T> StreamObserver<T> from(StreamObserver<T> outboundObserver) {
      AdvancingPhaser phaser = new AdvancingPhaser(1);
      CallStreamObserver<T> outboundCallStreamObserver = (CallStreamObserver<T>) outboundObserver;
      outboundCallStreamObserver.setOnReadyHandler(phaser::arrive);
      return new BufferingStreamObserver<>(
          phaser, outboundCallStreamObserver, executorService, bufferSize);
    }
  }
}
