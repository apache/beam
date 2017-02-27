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
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.BytesValue;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.Serializer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers as a consumer for data over the Beam Fn API. Multiplexes any received data
 * to all consumers in the specified output map.
 *
 * <p>Can be re-used serially across {@link org.apache.beam.fn.v1.BeamFnApi.ProcessBundleRequest}s.
 * For each request, call {@link #registerInputLocation()} to start and call
 * {@link #blockTillReadFinishes()} to finish.
 */
public class BeamFnDataReadRunner<OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataReadRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final BeamFnApi.ApiServiceDescriptor apiServiceDescriptor;
  private final Collection<ThrowingConsumer<WindowedValue<OutputT>>> consumers;
  private final Supplier<String> processBundleInstructionIdSupplier;
  private final BeamFnDataClient beamFnDataClientFactory;
  private final Coder<WindowedValue<OutputT>> coder;
  private final BeamFnApi.Target inputTarget;

  private CompletableFuture<Void> readFuture;

  public BeamFnDataReadRunner(
      BeamFnApi.FunctionSpec functionSpec,
      Supplier<String> processBundleInstructionIdSupplier,
      BeamFnApi.Target inputTarget,
      BeamFnApi.Coder coderSpec,
      BeamFnDataClient beamFnDataClientFactory,
      Map<String, Collection<ThrowingConsumer<WindowedValue<OutputT>>>> outputMap)
          throws IOException {
    this.apiServiceDescriptor = functionSpec.getData().unpack(BeamFnApi.RemoteGrpcPort.class)
        .getApiServiceDescriptor();
    this.inputTarget = inputTarget;
    this.processBundleInstructionIdSupplier = processBundleInstructionIdSupplier;
    this.beamFnDataClientFactory = beamFnDataClientFactory;
    this.consumers = ImmutableList.copyOf(FluentIterable.concat(outputMap.values()));

    @SuppressWarnings("unchecked")
    Coder<WindowedValue<OutputT>> coder = Serializer.deserialize(
        OBJECT_MAPPER.readValue(
            coderSpec.getFunctionSpec().getData().unpack(BytesValue.class).getValue().newInput(),
            Map.class),
        Coder.class);
    this.coder = coder;
  }

  public void registerInputLocation() {
    this.readFuture = beamFnDataClientFactory.forInboundConsumer(
        apiServiceDescriptor,
        KV.of(processBundleInstructionIdSupplier.get(), inputTarget),
        coder,
        this::multiplexToConsumers);
  }

  public void blockTillReadFinishes() throws Exception {
    LOG.debug("Waiting for process bundle instruction {} and target {} to close.",
        processBundleInstructionIdSupplier.get(), inputTarget);
    readFuture.get();
  }

  private void multiplexToConsumers(WindowedValue<OutputT> value) throws Exception {
    for (ThrowingConsumer<WindowedValue<OutputT>> consumer : consumers) {
      consumer.accept(value);
    }
  }
}
