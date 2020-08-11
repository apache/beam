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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.samza.config.JobConfig.JOB_ID;
import static org.apache.samza.config.JobConfig.JOB_NAME;
import static org.apache.samza.config.TaskConfig.COMMIT_MS;
import static org.apache.samza.config.TaskConfig.GROUPER_FACTORY;
import static org.apache.samza.config.TaskConfig.MAX_CONCURRENCY;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.samza.BeamJobCoordinatorRunner;
import org.apache.beam.runners.samza.SamzaExecutionEnvironment;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.container.BeamContainerRunner;
import org.apache.beam.runners.samza.runtime.SamzaStateInternals;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigLoaderFactory;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.config.loaders.PropertiesConfigLoaderFactory;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.job.yarn.YarnJobFactory;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.runtime.RemoteApplicationRunner;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.zk.ZkJobCoordinatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builder class to generate configs for BEAM samza runner during runtime. */
public class ConfigBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigBuilder.class);

  private static final String BEAM_STORE_FACTORY = "stores.beamStore.factory";
  private static final String APP_RUNNER_CLASS = "app.runner.class";
  private static final String YARN_PACKAGE_PATH = "yarn.package.path";
  private static final String JOB_FACTORY_CLASS = "job.factory.class";

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

  /** @return built configuration */
  public Config build() {
    try {
      // apply framework configs
      config.putAll(createSystemConfig(options, config));

      // apply user configs
      config.putAll(createUserConfig(options));

      config.put(ApplicationConfig.APP_NAME, options.getJobName());
      config.put(ApplicationConfig.APP_ID, options.getJobInstance());
      config.put(JOB_NAME, options.getJobName());
      config.put(JOB_ID, options.getJobInstance());
      config.put(MAX_CONCURRENCY, String.valueOf(options.getMaxBundleSize()));

      // remove config overrides before serialization (LISAMZA-15259)
      options.setConfigOverride(new HashMap<>());
      config.put(
          "beamPipelineOptions",
          Base64Serializer.serializeUnchecked(new SerializablePipelineOptions(options)));

      validateConfigs(options, config);

      return new MapConfig(config);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, String> createUserConfig(SamzaPipelineOptions options)
      throws Exception {
    final Map<String, String> config = new HashMap<>();

    // apply user configs
    final String configFilePath = options.getConfigFilePath();

    // If user provides a config file, use it as base configs.
    if (StringUtils.isNoneEmpty(configFilePath)) {
      LOG.info("configFilePath: " + configFilePath);

      final Config properties = new MapConfig(Collections.singletonMap("path", configFilePath));
      final ConfigLoaderFactory configLoaderFactory =
          options.getConfigLoaderFactory().getDeclaredConstructor().newInstance();

      LOG.info("configLoaderFactory: " + configLoaderFactory.getClass().getName());

      // Config file must exist for default properties config
      // TODO: add check to all non-empty files once we don't need to
      // pass the command-line args through the containers
      if (configLoaderFactory instanceof PropertiesConfigLoaderFactory) {
        checkArgument(
            new File(configFilePath).exists(), "Config file %s does not exist", configFilePath);
      }

      config.putAll(configLoaderFactory.getLoader(properties).getConfig());
    }
    // Apply override on top
    if (options.getConfigOverride() != null) {
      config.putAll(options.getConfigOverride());
    }

    return config;
  }

  private static void validateZKStandAloneRun(Map<String, String> config) {
    checkArgument(
        config.containsKey(APP_RUNNER_CLASS),
        "Config %s not found for %s Deployment",
        APP_RUNNER_CLASS,
        SamzaExecutionEnvironment.STANDALONE);
    checkArgument(
        config.get(APP_RUNNER_CLASS).equals(LocalApplicationRunner.class.getName()),
        "Config %s must be set to %s for %s Deployment",
        APP_RUNNER_CLASS,
        LocalApplicationRunner.class.getName(),
        SamzaExecutionEnvironment.STANDALONE);
    checkArgument(
        config.containsKey(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY),
        "Config %s not found for %s Deployment",
        JobCoordinatorConfig.JOB_COORDINATOR_FACTORY,
        SamzaExecutionEnvironment.STANDALONE);
    checkArgument(
        config
            .get(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY)
            .equals(ZkJobCoordinatorFactory.class.getName()),
        "Config %s must be set to %s for %s Deployment",
        JobCoordinatorConfig.JOB_COORDINATOR_FACTORY,
        ZkJobCoordinatorFactory.class.getName(),
        SamzaExecutionEnvironment.STANDALONE);
    checkArgument(
        config.containsKey(ZkConfig.ZK_CONNECT),
        "Config %s not found for %s Deployment",
        ZkConfig.ZK_CONNECT,
        SamzaExecutionEnvironment.STANDALONE);
  }

  private static void validateYarnRun(Map<String, String> config) {
    checkArgument(
        config.containsKey(YARN_PACKAGE_PATH),
        "Config %s not found for %s Deployment",
        YARN_PACKAGE_PATH,
        SamzaExecutionEnvironment.YARN);
    final String appRunner = config.get(APP_RUNNER_CLASS);
    checkArgument(
        appRunner == null
            || BeamJobCoordinatorRunner.class.getName().equals(appRunner)
            || RemoteApplicationRunner.class.getName().equals(appRunner)
            || BeamContainerRunner.class.getName().equals(appRunner),
        "Config %s must be set to %s for %s Deployment",
        APP_RUNNER_CLASS,
        RemoteApplicationRunner.class.getName(),
        SamzaExecutionEnvironment.YARN);
    checkArgument(
        config.containsKey(JOB_FACTORY_CLASS),
        "Config %s not found for %s Deployment",
        JOB_FACTORY_CLASS,
        SamzaExecutionEnvironment.YARN);
  }

  @VisibleForTesting
  public static Map<String, String> localRunConfig() {
    // Default Samza config using local deployment of a single JVM
    return ImmutableMap.<String, String>builder()
        .put(APP_RUNNER_CLASS, LocalApplicationRunner.class.getName())
        .put(
            JobCoordinatorConfig.JOB_COORDINATOR_FACTORY,
            PassthroughJobCoordinatorFactory.class.getName())
        .put(GROUPER_FACTORY, SingleContainerGrouperFactory.class.getName())
        .put(COMMIT_MS, "-1")
        .put("processor.id", "1")
        .put(
            // TODO: remove after SAMZA-1531 is resolved
            ApplicationConfig.APP_RUN_ID,
            System.currentTimeMillis()
                + "-"
                // use the most significant bits in UUID (8 digits) to avoid collision
                + UUID.randomUUID().toString().substring(0, 8))
        .build();
  }

  public static Map<String, String> yarnRunConfig() {
    // Default Samza config using yarn deployment
    return ImmutableMap.<String, String>builder()
        .put(APP_RUNNER_CLASS, RemoteApplicationRunner.class.getName())
        .put(JOB_FACTORY_CLASS, YarnJobFactory.class.getName())
        .build();
  }

  public static Map<String, String> standAloneRunConfig() {
    // Default Samza config using stand alone deployment
    return ImmutableMap.<String, String>builder()
        .put(APP_RUNNER_CLASS, LocalApplicationRunner.class.getName())
        .put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, ZkJobCoordinatorFactory.class.getName())
        .build();
  }

  private static Map<String, String> createSystemConfig(
      SamzaPipelineOptions options, Map<String, String> config) {
    final ImmutableMap.Builder<String, String> configBuilder =
        ImmutableMap.<String, String>builder()
            .put("stores.beamStore.key.serde", "byteArraySerde")
            .put("stores.beamStore.msg.serde", "stateValueSerde")
            .put(
                "serializers.registry.stateValueSerde.class",
                SamzaStateInternals.StateValueSerdeFactory.class.getName())
            .put(
                "serializers.registry.byteArraySerde.class",
                SamzaStateInternals.ByteArraySerdeFactory.class.getName());

    // if config does not contain "stores.beamStore.factory" at this moment,
    // then it is a stateless job.
    if (!config.containsKey(BEAM_STORE_FACTORY)) {
      options.setStateDurable(false);
      configBuilder.put(
          BEAM_STORE_FACTORY,
          "org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory");
    }

    LOG.info("Execution environment is " + options.getSamzaExecutionEnvironment());
    switch (options.getSamzaExecutionEnvironment()) {
      case YARN:
        configBuilder.putAll(yarnRunConfig());
        break;
      case STANDALONE:
        configBuilder.putAll(standAloneRunConfig());
        break;
      default: // LOCAL
        configBuilder.putAll(localRunConfig());
        break;
    }

    // TODO: remove after we sort out Samza task wrapper
    configBuilder.put("samza.li.task.wrapper.enabled", "false");

    return configBuilder.build();
  }

  static Map<String, String> createRocksDBStoreConfig(SamzaPipelineOptions options) {
    final ImmutableMap.Builder<String, String> configBuilder =
        ImmutableMap.<String, String>builder()
            .put(
                BEAM_STORE_FACTORY,
                "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");

    if (options.getStateDurable()) {
      LOG.info("stateDurable is enabled");
      configBuilder.put("stores.beamStore.changelog", getChangelogTopic(options, "beamStore"));
      configBuilder.put("job.host-affinity.enabled", "true");
    }

    return configBuilder.build();
  }

  private static void validateConfigs(SamzaPipelineOptions options, Map<String, String> config) {

    // validate execution environment
    switch (options.getSamzaExecutionEnvironment()) {
      case YARN:
        validateYarnRun(config);
        break;
      case STANDALONE:
        validateZKStandAloneRun(config);
        break;
      default:
        // do nothing
        break;
    }
  }

  static String getChangelogTopic(SamzaPipelineOptions options, String storeName) {
    return String.format(
        "%s-%s-%s-changelog", options.getJobName(), options.getJobInstance(), storeName);
  }
}
