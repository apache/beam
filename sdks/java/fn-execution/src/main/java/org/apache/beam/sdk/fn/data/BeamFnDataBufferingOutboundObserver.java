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
package org.apache.beam.sdk.fn.data;

import java.util.Collections;
import java.util.List;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/**
 * A buffering outbound {@link FnDataReceiver} for the Beam Fn Data API.
 *
 * <p>Encodes individually consumed elements with the provided {@link Coder} producing a single
 * {@link BeamFnApi.Elements} message when the buffer threshold is surpassed.
 *
 * <p>The default size-based buffer threshold can be overridden by specifying the experiment {@code
 * data_buffer_size_limit=<bytes>}
 *
 * <p>The default time-based buffer threshold can be overridden by specifying the experiment {@code
 * data_buffer_time_limit_ms=<milliseconds>}
 */
public interface BeamFnDataBufferingOutboundObserver<T> extends CloseableFnDataReceiver<T> {
  // TODO: Consider moving this constant out of this interface
  /** @deprecated Use DATA_BUFFER_SIZE_LIMIT instead. */
  @Deprecated String BEAM_FN_API_DATA_BUFFER_LIMIT = "beam_fn_api_data_buffer_limit=";

  /** @deprecated Use DATA_BUFFER_SIZE_LIMIT instead. */
  @Deprecated String BEAM_FN_API_DATA_BUFFER_SIZE_LIMIT = "beam_fn_api_data_buffer_size_limit=";

  String DATA_BUFFER_SIZE_LIMIT = "data_buffer_size_limit=";
  @VisibleForTesting int DEFAULT_BUFFER_LIMIT_BYTES = 1_000_000;

  /** @deprecated Use DATA_BUFFER_TIME_LIMIT_MS instead. */
  @Deprecated String BEAM_FN_API_DATA_BUFFER_TIME_LIMIT = "beam_fn_api_data_buffer_time_limit=";

  String DATA_BUFFER_TIME_LIMIT_MS = "data_buffer_time_limit_ms=";
  long DEFAULT_BUFFER_LIMIT_TIME_MS = -1L;

  static <T> BeamFnDataSizeBasedBufferingOutboundObserver<T> forLocation(
      PipelineOptions options,
      LogicalEndpoint endpoint,
      Coder<T> coder,
      StreamObserver<BeamFnApi.Elements> outboundObserver) {
    int sizeLimit = getSizeLimit(options);
    long timeLimit = getTimeLimit(options);
    if (timeLimit > 0) {
      return new BeamFnDataTimeBasedBufferingOutboundObserver<>(
          sizeLimit, timeLimit, endpoint, coder, outboundObserver);
    } else {
      return new BeamFnDataSizeBasedBufferingOutboundObserver<>(
          sizeLimit, endpoint, coder, outboundObserver);
    }
  }

  static int getSizeLimit(PipelineOptions options) {
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    for (String experiment : experiments == null ? Collections.<String>emptyList() : experiments) {
      if (experiment.startsWith(DATA_BUFFER_SIZE_LIMIT)) {
        return Integer.parseInt(experiment.substring(DATA_BUFFER_SIZE_LIMIT.length()));
      }
      if (experiment.startsWith(BEAM_FN_API_DATA_BUFFER_SIZE_LIMIT)) {
        return Integer.parseInt(experiment.substring(BEAM_FN_API_DATA_BUFFER_SIZE_LIMIT.length()));
      }
      if (experiment.startsWith(BEAM_FN_API_DATA_BUFFER_LIMIT)) {
        return Integer.parseInt(experiment.substring(BEAM_FN_API_DATA_BUFFER_LIMIT.length()));
      }
    }
    return DEFAULT_BUFFER_LIMIT_BYTES;
  }

  static long getTimeLimit(PipelineOptions options) {
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    for (String experiment : experiments == null ? Collections.<String>emptyList() : experiments) {
      if (experiment.startsWith(DATA_BUFFER_TIME_LIMIT_MS)) {
        return Long.parseLong(experiment.substring(DATA_BUFFER_TIME_LIMIT_MS.length()));
      }
      if (experiment.startsWith(BEAM_FN_API_DATA_BUFFER_TIME_LIMIT)) {
        return Long.parseLong(experiment.substring(BEAM_FN_API_DATA_BUFFER_TIME_LIMIT.length()));
      }
    }
    return DEFAULT_BUFFER_LIMIT_TIME_MS;
  }
}
