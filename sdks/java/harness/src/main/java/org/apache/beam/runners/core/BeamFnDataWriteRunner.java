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

package org.apache.beam.runners.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.CloseableThrowingConsumer;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.Serializer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

/**
 * Registers as a consumer with the Beam Fn Data API. Propagates and elements consumed to
 * the the registered consumer.
 *
 * <p>Can be re-used serially across {@link org.apache.beam.fn.v1.BeamFnApi.ProcessBundleRequest}s.
 * For each request, call {@link #registerForOutput()} to start and call {@link #close()} to finish.
 */
public class BeamFnDataWriteRunner<InputT> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final BeamFnApi.ApiServiceDescriptor apiServiceDescriptor;
  private final BeamFnApi.Target outputTarget;
  private final Coder<WindowedValue<InputT>> coder;
  private final BeamFnDataClient beamFnDataClientFactory;
  private final Supplier<String> processBundleInstructionIdSupplier;

  private CloseableThrowingConsumer<WindowedValue<InputT>> consumer;

  public BeamFnDataWriteRunner(
      BeamFnApi.FunctionSpec functionSpec,
      Supplier<String> processBundleInstructionIdSupplier,
      BeamFnApi.Target outputTarget,
      BeamFnApi.Coder coderSpec,
      BeamFnDataClient beamFnDataClientFactory)
          throws IOException {
    this.apiServiceDescriptor = functionSpec.getData().unpack(BeamFnApi.RemoteGrpcPort.class)
        .getApiServiceDescriptor();
    this.beamFnDataClientFactory = beamFnDataClientFactory;
    this.processBundleInstructionIdSupplier = processBundleInstructionIdSupplier;
    this.outputTarget = outputTarget;

    @SuppressWarnings("unchecked")
    Coder<WindowedValue<InputT>> coder = Serializer.deserialize(
        OBJECT_MAPPER.readValue(
            coderSpec.getFunctionSpec().getData().unpack(BytesValue.class).getValue().newInput(),
            Map.class),
        Coder.class);
    this.coder = coder;
  }

  public void registerForOutput() {
    consumer = beamFnDataClientFactory.forOutboundConsumer(
        apiServiceDescriptor,
        KV.of(processBundleInstructionIdSupplier.get(), outputTarget),
        coder);
  }

  public void close() throws Exception {
    consumer.close();
  }

  public void consume(WindowedValue<InputT> value) throws Exception {
    consumer.accept(value);
  }
}
