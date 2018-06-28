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

package org.apache.beam.runners.samza.translation;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.adapter.BoundedSourceSystem;
import org.apache.beam.runners.samza.adapter.UnboundedSourceSystem;
import org.apache.beam.runners.samza.util.Base64Serializer;
import org.apache.beam.runners.samza.util.SamzaCoders;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.factories.PropertiesConfigFactory;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.ByteSerdeFactory;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;

/** Builder class to generate configs for BEAM samza runner during runtime. */
public class ConfigBuilder extends Pipeline.PipelineVisitor.Defaults {
  private static final String APP_RUNNER_CLASS = "app.runner.class";

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Map<PValue, String> idMap;
  private final Map<String, String> config = new HashMap<>();
  private final Pipeline pipeline;
  private boolean foundSource = false;

  public static Config buildConfig(
      Pipeline pipeline, SamzaPipelineOptions options, Map<PValue, String> idMap) {
    try {
      final ConfigBuilder builder = new ConfigBuilder(idMap, pipeline);
      pipeline.traverseTopologically(builder);
      builder.checkFoundSource();
      final Map<String, String> config = new HashMap<>(builder.getConfig());

      createConfigForSystemStore(config);

      // apply user configs
      config.putAll(createUserConfig(options));

      config.put(JobConfig.JOB_NAME(), options.getJobName());
      config.put(
          "beamPipelineOptions",
          Base64Serializer.serializeUnchecked(new SerializablePipelineOptions(options)));
      // TODO: remove after SAMZA-1531 is resolved
      config.put(
          ApplicationConfig.APP_RUN_ID,
          String.valueOf(System.currentTimeMillis())
              + "-"
              // use the most significant bits in UUID (8 digits) to avoid collision
              + UUID.randomUUID().toString().substring(0, 8));

      return new MapConfig(config);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, String> createUserConfig(SamzaPipelineOptions options) {
    final String configFilePath = options.getConfigFilePath();
    final Map<String, String> config = new HashMap<>();

    // If user provides a config file, use it as base configs.
    if (StringUtils.isNoneEmpty(configFilePath)) {
      final File configFile = new File(configFilePath);
      checkArgument(configFile.exists(), "Config file %s does not exist", configFilePath);
      final PropertiesConfigFactory configFactory = new PropertiesConfigFactory();
      final URI configUri = configFile.toURI();
      config.putAll(configFactory.getConfig(configUri));
    }

    // Apply override on top
    if (options.getConfigOverride() != null) {
      config.putAll(options.getConfigOverride());
    }

    // If the config is empty, use the default local running mode
    if (config.isEmpty()) {
      config.putAll(localRunConfig());
    }

    return config;
  }

  @VisibleForTesting
  public static Map<String, String> localRunConfig() {
    // Default Samza config using local deployment of a single JVM
    return ImmutableMap.<String, String>builder()
        .put(APP_RUNNER_CLASS, LocalApplicationRunner.class.getName())
        .put(
            JobCoordinatorConfig.JOB_COORDINATOR_FACTORY,
            PassthroughJobCoordinatorFactory.class.getName())
        .put(
            JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY,
            PassthroughCoordinationUtilsFactory.class.getName())
        .put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName())
        .put(TaskConfig.COMMIT_MS(), "-1")
        .put("processor.id", "1")
        .build();
  }

  private static void createConfigForSystemStore(Map<String, String> config) {
    config.put(
        "stores.beamStore.factory",
        "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
    config.put("stores.beamStore.key.serde", "byteSerde");
    config.put("stores.beamStore.msg.serde", "byteSerde");
    config.put("serializers.registry.byteSerde.class", ByteSerdeFactory.class.getName());
  }

  private ConfigBuilder(Map<PValue, String> idMap, Pipeline pipeline) {
    this.idMap = idMap;
    this.pipeline = pipeline;
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    if (node.getTransform() instanceof Read.Bounded) {
      foundSource = true;
      processReadBounded(node, (Read.Bounded<?>) node.getTransform());
    } else if (node.getTransform() instanceof Read.Unbounded) {
      foundSource = true;
      processReadUnbounded(node, (Read.Unbounded<?>) node.getTransform());
    } else if (node.getTransform() instanceof ParDo.MultiOutput) {
      processParDo((ParDo.MultiOutput<?, ?>) node.getTransform());
    }
  }

  private <T> void processReadBounded(TransformHierarchy.Node node, Read.Bounded<T> transform) {
    final String id = getId(Iterables.getOnlyElement(node.getOutputs().values()));
    final BoundedSource<T> source = transform.getSource();

    @SuppressWarnings("unchecked")
    final PCollection<T> output =
        (PCollection<T>)
            Iterables.getOnlyElement(node.toAppliedPTransform(pipeline).getOutputs().values());
    final Coder<WindowedValue<T>> coder = SamzaCoders.of(output);

    config.putAll(BoundedSourceSystem.createConfigFor(id, source, coder, node.getFullName()));
  }

  private <T> void processReadUnbounded(TransformHierarchy.Node node, Read.Unbounded<T> transform) {
    final String id = getId(Iterables.getOnlyElement(node.getOutputs().values()));
    final UnboundedSource<T, ?> source = transform.getSource();

    @SuppressWarnings("unchecked")
    final PCollection<T> output =
        (PCollection<T>)
            Iterables.getOnlyElement(node.toAppliedPTransform(pipeline).getOutputs().values());
    final Coder<WindowedValue<T>> coder = SamzaCoders.of(output);

    config.putAll(UnboundedSourceSystem.createConfigFor(id, source, coder, node.getFullName()));
  }

  private void processParDo(ParDo.MultiOutput<?, ?> parDo) {
    final DoFnSignature signature = DoFnSignatures.getSignature(parDo.getFn().getClass());
    if (signature.usesState()) {
      // set up user state configs
      for (DoFnSignature.StateDeclaration state : signature.stateDeclarations().values()) {
        String storeId = state.id();
        config.put(
            "stores." + storeId + ".factory",
            "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        config.put("stores." + storeId + ".key.serde", "byteSerde");
        config.put("stores." + storeId + ".msg.serde", "byteSerde");
      }
    }
  }

  private String getId(PValue pvalue) {
    final String id = idMap.get(pvalue);
    if (id == null) {
      throw new IllegalStateException(String.format("Could not find id for pvalue: %s", pvalue));
    }
    return id;
  }

  private void checkFoundSource() {
    if (!foundSource) {
      throw new IllegalStateException("Could not find any sources in pipeline!");
    }
  }

  private Map<String, String> getConfig() {
    return config;
  }

  private String writeValueAsJsonString(Object object) {
    try {
      return objectMapper.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
