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
package org.apache.beam.fn.harness.data;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.beam.fn.harness.fn.CloseableThrowingConsumer;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A buffering outbound {@link Consumer} for the Beam Fn Data API.
 *
 * <p>Encodes individually consumed elements with the provided {@link Coder} producing
 * a single {@link org.apache.beam.fn.v1.BeamFnApi.Elements} message when the buffer threshold
 * is surpassed.
 *
 * <p>The default buffer threshold can be overridden by specifying the experiment
 * {@code beam_fn_api_data_buffer_limit=<bytes>}
 *
 * <p>TODO: Handle outputting large elements (&gt; 2GiBs). Note that this also applies to the
 * input side as well.
 *
 * <p>TODO: Handle outputting elements that are zero bytes by outputting a single byte as
 * a marker, detect on the input side that no bytes were read and force reading a single byte.
 */
public class BeamFnDataBufferingOutboundObserver<T>
    implements CloseableThrowingConsumer<WindowedValue<T>> {
  private static final String BEAM_FN_API_DATA_BUFFER_LIMIT = "beam_fn_api_data_buffer_limit=";
  private static final int DEFAULT_BUFFER_LIMIT_BYTES = 1_000_000;
  private static final Logger LOG =
      LoggerFactory.getLogger(BeamFnDataBufferingOutboundObserver.class);

  private long byteCounter;
  private long counter;
  private final int bufferLimit;
  private final Coder<WindowedValue<T>> coder;
  private final KV<String, BeamFnApi.Target> outputLocation;
  private final StreamObserver<BeamFnApi.Elements> outboundObserver;
  private final ByteString.Output bufferedElements;

  public BeamFnDataBufferingOutboundObserver(
      PipelineOptions options,
      KV<String, BeamFnApi.Target> outputLocation,
      Coder<WindowedValue<T>> coder,
      StreamObserver<BeamFnApi.Elements> outboundObserver) {
    this.bufferLimit = getBufferLimit(options);
    this.outputLocation = outputLocation;
    this.coder = coder;
    this.outboundObserver = outboundObserver;
    this.bufferedElements = ByteString.newOutput();
  }

  /**
   * Returns the {@code beam_fn_api_data_buffer_limit=<int>} experiment value if set. Otherwise
   * returns the default buffer limit.
   */
  private static int getBufferLimit(PipelineOptions options) {
    List<String> experiments = options.as(DataflowPipelineDebugOptions.class).getExperiments();
    for (String experiment : experiments == null ? Collections.<String>emptyList() : experiments) {
      if (experiment.startsWith(BEAM_FN_API_DATA_BUFFER_LIMIT)) {
        return Integer.parseInt(experiment.substring(BEAM_FN_API_DATA_BUFFER_LIMIT.length()));
      }
    }
    return DEFAULT_BUFFER_LIMIT_BYTES;
  }

  @Override
  public void close() throws Exception {
    BeamFnApi.Elements.Builder elements = convertBufferForTransmission();
    // This will add an empty data block representing the end of stream.
    elements.addDataBuilder()
        .setInstructionReference(outputLocation.getKey())
        .setTarget(outputLocation.getValue());

    LOG.debug("Closing stream for instruction {} and "
        + "target {} having transmitted {} values {} bytes",
        outputLocation.getKey(),
        outputLocation.getValue(),
        counter,
        byteCounter);
    outboundObserver.onNext(elements.build());
  }

  @Override
  public void accept(WindowedValue<T> t) throws IOException {
    coder.encode(t, bufferedElements, Context.NESTED);
    counter += 1;
    if (bufferedElements.size() >= bufferLimit) {
      outboundObserver.onNext(convertBufferForTransmission().build());
    }
  }

  private BeamFnApi.Elements.Builder convertBufferForTransmission() {
    BeamFnApi.Elements.Builder elements = BeamFnApi.Elements.newBuilder();
    if (bufferedElements.size() == 0) {
      return elements;
    }

    elements.addDataBuilder()
        .setInstructionReference(outputLocation.getKey())
        .setTarget(outputLocation.getValue())
        .setData(bufferedElements.toByteString());

    byteCounter += bufferedElements.size();
    bufferedElements.reset();
    return elements;
  }
}
