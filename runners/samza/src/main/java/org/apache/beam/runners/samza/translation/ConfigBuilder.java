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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.runners.samza.SamzaRunnerOverrideConfigs;
import org.apache.beam.runners.samza.util.Base64Serializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigFactory;
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
public class ConfigBuilder {
  private static final String APP_RUNNER_CLASS = "app.runner.class";

  private final Map<String, String> config = new HashMap<>();
  private final SamzaPipelineOptions options;

  public ConfigBuilder(SamzaPipelineOptions options) {
    this.options = options;
  }

  public void put(String name, String property) {
    config.put(name, property);
  }

  public void putAll(Map<String, String> properties) {
    config.putAll(properties);
  }

  public Config build() {
    try {
      config.putAll(systemStoreConfig());

      // apply user configs
      config.putAll(createUserConfig(options));

      config.put(JobConfig.JOB_NAME(), options.getJobName());
      config.put(
          "beamPipelineOptions",
          Base64Serializer.serializeUnchecked(new SerializablePipelineOptions(options)));

      return new MapConfig(config);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isEmptyUserConfig(Map<String, String> config) {
    if (config == null) {
      return true;
    }
    return config
        .keySet()
        .stream()
        .allMatch(key -> key.startsWith(SamzaRunnerOverrideConfigs.BEAM_RUNNER_CONFIG_PREFIX));
  }

  private static Map<String, String> createUserConfig(SamzaPipelineOptions options)
      throws Exception {
    final String configFilePath = options.getConfigFilePath();
    final Map<String, String> config = new HashMap<>();

    // If user provides a config file, use it as base configs.
    if (StringUtils.isNoneEmpty(configFilePath)) {
      final File configFile = new File(configFilePath);
      final URI configUri = configFile.toURI();
      final ConfigFactory configFactory =
          options.getConfigFactory().getDeclaredConstructor().newInstance();

      // Config file must exist for default properties config
      // TODO: add check to all non-empty files once we don't need to
      // pass the command-line args through the containers
      if (configFactory instanceof PropertiesConfigFactory) {
        checkArgument(configFile.exists(), "Config file %s does not exist", configFilePath);
      }

      config.putAll(configFactory.getConfig(configUri));
    }

    // Apply override on top
    if (options.getConfigOverride() != null) {
      config.putAll(options.getConfigOverride());
    }

    // If there is no user specified config, use the default local running mode
    // we are keeping this work around until https://issues.apache.org/jira/browse/BEAM-5732 is addressed
    if (isEmptyUserConfig(options.getConfigOverride())) {
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
        .put(
            // TODO: remove after SAMZA-1531 is resolved
            ApplicationConfig.APP_RUN_ID,
            String.valueOf(System.currentTimeMillis())
                + "-"
                // use the most significant bits in UUID (8 digits) to avoid collision
                + UUID.randomUUID().toString().substring(0, 8))
        .build();
  }

  private static Map<String, String> systemStoreConfig() {
    return ImmutableMap.<String, String>builder()
        .put(
            "stores.beamStore.factory",
            "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory")
        .put("stores.beamStore.key.serde", "byteSerde")
        .put("stores.beamStore.msg.serde", "byteSerde")
        .put("serializers.registry.byteSerde.class", ByteSerdeFactory.class.getName())
        .build();
  }
}
