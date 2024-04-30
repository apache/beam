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
package org.apache.beam.runners.dataflow.worker.streaming.config;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.api.services.dataflow.model.MapTask;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetConfigResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetConfigResponse.NameMapEntry;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fetches computation config from Streaming Appliance. */
@Internal
@ThreadSafe
public final class StreamingApplianceConfigFetcher implements ComputationConfig.Fetcher {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingApplianceConfigFetcher.class);

  private final WindmillServerStub windmillServer;
  private final Consumer<StreamingPipelineConfig> onPipelineConfig;
  private final ConcurrentHashMap<String, String> systemNameToComputationIdMap;
  private final Function<MapTask, MapTask> fixMapTaskMultiOutputInfoFn;

  public StreamingApplianceConfigFetcher(
      WindmillServerStub windmillServer,
      Consumer<StreamingPipelineConfig> onPipelineConfig,
      Function<MapTask, MapTask> fixMapTaskMultiOutputInfoFn) {
    this.windmillServer = windmillServer;
    this.onPipelineConfig = onPipelineConfig;
    this.fixMapTaskMultiOutputInfoFn = fixMapTaskMultiOutputInfoFn;
    this.systemNameToComputationIdMap = new ConcurrentHashMap<>();
  }

  private static Table<String, String, String> transformUserNameToStateFamilyByComputationId(
      Windmill.GetConfigResponse response) {
    // a row in the table is <ComputationId, TransformUserName, StateFamilyName>
    Table<String, String, String> computationIdTransformUserNameStateFamilyNameTable =
        HashBasedTable.create();
    for (Windmill.GetConfigResponse.ComputationConfigMapEntry computationConfig :
        response.getComputationConfigMapList()) {
      for (Windmill.ComputationConfig.TransformUserNameToStateFamilyEntry entry :
          computationConfig.getComputationConfig().getTransformUserNameToStateFamilyList()) {
        computationIdTransformUserNameStateFamilyNameTable.put(
            computationConfig.getComputationId(),
            entry.getTransformUserName(),
            entry.getStateFamily());
      }
    }

    return computationIdTransformUserNameStateFamilyNameTable;
  }

  /** Deserialize {@link MapTask} and populate MultiOutputInfos in MapTask. */
  private Optional<MapTask> deserializeAndFixMapTask(String serializedMapTask) {
    try {
      return Optional.of(
          fixMapTaskMultiOutputInfoFn.apply(
              Transport.getJsonFactory().fromString(serializedMapTask, MapTask.class)));
    } catch (IOException e) {
      LOG.warn("Parsing MapTask failed: {}", serializedMapTask, e);
    }
    return Optional.empty();
  }

  @Override
  public Optional<ComputationConfig> getConfig(String computationId) {
    Preconditions.checkArgument(
        !computationId.isEmpty(),
        "computationId is empty. Cannot fetch computation config without a computationId.");

    GetConfigResponse response =
        windmillServer.getConfig(
            Windmill.GetConfigRequest.newBuilder().addComputations(computationId).build());

    if (response == null) {
      return Optional.empty();
    }

    for (Windmill.GetConfigResponse.SystemNameToComputationIdMapEntry entry :
        response.getSystemNameToComputationIdMapList()) {
      systemNameToComputationIdMap.put(entry.getSystemName(), entry.getComputationId());
    }

    onPipelineConfig.accept(
        StreamingPipelineConfig.forAppliance(
            response.getNameMapList().stream()
                .collect(toImmutableMap(NameMapEntry::getUserName, NameMapEntry::getSystemName))));

    return createComputationConfig(
        // We are only fetching the config for 1 computation, so we should only be getting that
        // computation back.
        Iterables.getOnlyElement(response.getCloudWorksList()),
        transformUserNameToStateFamilyByComputationId(response));
  }

  private Optional<ComputationConfig> createComputationConfig(
      String serializedMapTask,
      Table<String, String, String> transformUserNameToStateFamilyByComputationId) {
    return deserializeAndFixMapTask(serializedMapTask)
        .map(
            mapTask -> {
              String computationId =
                  systemNameToComputationIdMap.getOrDefault(
                      mapTask.getSystemName(), mapTask.getSystemName());
              return ComputationConfig.create(
                  mapTask, transformUserNameToStateFamilyByComputationId.row(computationId));
            });
  }
}
