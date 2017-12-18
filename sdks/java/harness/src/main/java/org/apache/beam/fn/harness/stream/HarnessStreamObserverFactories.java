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

import io.grpc.stub.StreamObserver;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.fn.stream.StreamObserverFactory;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Uses {@link PipelineOptions} to configure which underlying {@link StreamObserver} implementation
 * to use in the java SDK harness.
 */
public abstract class HarnessStreamObserverFactories {
  public static org.apache.beam.sdk.fn.stream.StreamObserverFactory fromOptions(
      PipelineOptions options) {
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    if (experiments != null && experiments.contains("beam_fn_api_buffered_stream")) {
      int bufferSize = getBufferSize(experiments);
      if (bufferSize > 0) {
        return StreamObserverFactory.buffered(
            options.as(GcsOptions.class).getExecutorService(), bufferSize);
      }
      return StreamObserverFactory.buffered(
          options.as(GcsOptions.class).getExecutorService());
    }
    return StreamObserverFactory.direct();
  }

  private static int getBufferSize(List<String> experiments) {
    for (String experiment : experiments) {
      if (experiment.startsWith("beam_fn_api_buffered_stream_buffer_size=")) {
        return Integer.parseInt(
            experiment.substring("beam_fn_api_buffered_stream_buffer_size=".length()));
      }
    }
    return -1;
  }
}
