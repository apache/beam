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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.runners.samza.SamzaExecutionEnvironment;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.job.yarn.YarnJobFactory;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.runtime.RemoteApplicationRunner;
import org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory;
import org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory;
import org.apache.samza.zk.ZkJobCoordinatorFactory;
import org.junit.Test;

/** Test config generations for {@link org.apache.beam.runners.samza.SamzaRunner}. */
// TODO(https://github.com/apache/beam/issues/21230): Remove when new version of errorprone is
// released (2.11.0)
@SuppressWarnings("unused")
public class ConfigGeneratorTest {
  private static final String APP_RUNNER_CLASS = "app.runner.class";
  private static final String JOB_FACTORY_CLASS = "job.factory.class";

  @Test
  public void testStatefulBeamStoreConfig() {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setJobName("TestStoreConfig");
    options.setRunner(SamzaRunner.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1, 2, 3)).apply(Sum.integersGlobally());

    pipeline.replaceAll(SamzaTransformOverrides.getDefaultOverrides());

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);
    final Set<String> nonUniqueStateIds = StateIdParser.scan(pipeline);
    final ConfigContext configCtx = new ConfigContext(idMap, nonUniqueStateIds, options);
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Config config = configBuilder.build();

    assertEquals(
        RocksDbKeyValueStorageEngineFactory.class.getName(),
        config.get("stores.beamStore.factory"));
    assertEquals("byteArraySerde", config.get("stores.beamStore.key.serde"));
    assertEquals("stateValueSerde", config.get("stores.beamStore.msg.serde"));
    assertNull(config.get("stores.beamStore.changelog"));

    options.setStateDurable(true);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Config config2 = configBuilder.build();
    assertEquals(
        "TestStoreConfig-1-beamStore-changelog", config2.get("stores.beamStore.changelog"));
  }

  @Test
  public void testStatelessBeamStoreConfig() {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setJobName("TestStoreConfig");
    options.setRunner(SamzaRunner.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Impulse.create()).apply(Filter.by(Objects::nonNull));

    pipeline.replaceAll(SamzaTransformOverrides.getDefaultOverrides());

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);
    final Set<String> nonUniqueStateIds = StateIdParser.scan(pipeline);
    final ConfigContext configCtx = new ConfigContext(idMap, nonUniqueStateIds, options);
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Config config = configBuilder.build();

    assertEquals(
        InMemoryKeyValueStorageEngineFactory.class.getName(),
        config.get("stores.beamStore.factory"));
    assertEquals("byteArraySerde", config.get("stores.beamStore.key.serde"));
    assertEquals("stateValueSerde", config.get("stores.beamStore.msg.serde"));
    assertNull(config.get("stores.beamStore.changelog"));

    options.setStateDurable(true);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Config config2 = configBuilder.build();
    // For stateless jobs, ignore state durable pipeline option.
    assertNull(config2.get("stores.beamStore.changelog"));
  }

  @Test
  public void testSamzaLocalExecutionEnvironmentConfig() {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setJobName("TestEnvConfig");
    options.setRunner(SamzaRunner.class);
    options.setSamzaExecutionEnvironment(SamzaExecutionEnvironment.LOCAL);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1, 2, 3)).apply(Sum.integersGlobally());

    pipeline.replaceAll(SamzaTransformOverrides.getDefaultOverrides());

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);
    final Set<String> nonUniqueStateIds = StateIdParser.scan(pipeline);
    final ConfigContext configCtx = new ConfigContext(idMap, nonUniqueStateIds, options);
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Config config = configBuilder.build();

    assertTrue(
        Maps.difference(config, ConfigBuilder.localRunConfig()).entriesOnlyOnRight().isEmpty());
  }

  @Test
  public void testSamzaYarnExecutionEnvironmentConfig() {
    final String yarnPackagePath = "yarn.package.path";
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setJobName("TestEnvConfig");
    options.setRunner(SamzaRunner.class);
    options.setSamzaExecutionEnvironment(SamzaExecutionEnvironment.YARN);
    options.setConfigOverride(
        ImmutableMap.<String, String>builder()
            .put(
                yarnPackagePath,
                "file://${basedir}/target/${project.artifactId}-${pom.version}-dist.tar.gz")
            .build());

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1, 2, 3)).apply(Sum.integersGlobally());

    pipeline.replaceAll(SamzaTransformOverrides.getDefaultOverrides());

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);
    final Set<String> nonUniqueStateIds = StateIdParser.scan(pipeline);
    final ConfigContext configCtx = new ConfigContext(idMap, nonUniqueStateIds, options);
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    try {
      Config config = configBuilder.build();
      assertEquals(config.get(APP_RUNNER_CLASS), RemoteApplicationRunner.class.getName());
      assertEquals(config.get(JOB_FACTORY_CLASS), YarnJobFactory.class.getName());
    } catch (IllegalArgumentException e) {
      throw new AssertionError(
          String.format(
              "Failed to validate correct configs for %s samza execution environment",
              SamzaExecutionEnvironment.YARN),
          e);
    }
  }

  @Test
  public void testSamzaStandAloneExecutionEnvironmentConfig() {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setJobName("TestEnvConfig");
    options.setRunner(SamzaRunner.class);
    options.setSamzaExecutionEnvironment(SamzaExecutionEnvironment.STANDALONE);
    options.setConfigOverride(
        ImmutableMap.<String, String>builder().put(ZkConfig.ZK_CONNECT, "localhost:2181").build());

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1, 2, 3)).apply(Sum.integersGlobally());

    pipeline.replaceAll(SamzaTransformOverrides.getDefaultOverrides());

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);
    final Set<String> nonUniqueStateIds = StateIdParser.scan(pipeline);
    final ConfigContext configCtx = new ConfigContext(idMap, nonUniqueStateIds, options);
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    try {
      Config config = configBuilder.build();
      assertEquals(config.get(APP_RUNNER_CLASS), LocalApplicationRunner.class.getName());
      assertEquals(
          config.get(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY),
          ZkJobCoordinatorFactory.class.getName());
    } catch (IllegalArgumentException e) {
      throw new AssertionError(
          String.format(
              "Failed to validate correct configs for %s samza execution environment",
              SamzaExecutionEnvironment.STANDALONE),
          e);
    }
  }

  @Test
  public void testUserStoreConfig() {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setJobName("TestStoreConfig");
    options.setRunner(SamzaRunner.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            Create.empty(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings())))
        .apply(
            ParDo.of(
                new DoFn<KV<String, String>, Void>() {
                  private static final String testState = "testState";

                  @StateId(testState)
                  private final StateSpec<ValueState<Integer>> state = StateSpecs.value();

                  @ProcessElement
                  public void processElement(
                      ProcessContext context, @StateId(testState) ValueState<Integer> state) {}
                }));

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);
    final Set<String> nonUniqueStateIds = StateIdParser.scan(pipeline);
    final ConfigContext configCtx = new ConfigContext(idMap, nonUniqueStateIds, options);
    final ConfigBuilder configBuilder = new ConfigBuilder(options);

    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Config config = configBuilder.build();

    assertEquals(
        RocksDbKeyValueStorageEngineFactory.class.getName(),
        config.get("stores.testState.factory"));
    assertEquals("byteArraySerde", config.get("stores.testState.key.serde"));
    assertEquals("stateValueSerde", config.get("stores.testState.msg.serde"));
    assertNull(config.get("stores.testState.changelog"));

    options.setStateDurable(true);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Config config2 = configBuilder.build();
    assertEquals(
        "TestStoreConfig-1-testState-changelog", config2.get("stores.testState.changelog"));
  }

  @Test
  public void testUserStoreConfigSameStateIdAcrossParDo() {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setJobName("TestStoreConfig");
    options.setRunner(SamzaRunner.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            Create.empty(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings())))
        .apply(
            "First stateful ParDo",
            ParDo.of(
                new DoFn<KV<String, String>, KV<String, String>>() {
                  private static final String testState = "testState";

                  @StateId(testState)
                  private final StateSpec<ValueState<Integer>> state = StateSpecs.value();

                  @ProcessElement
                  public void processElement(
                      ProcessContext context, @StateId(testState) ValueState<Integer> state) {
                    context.output(context.element());
                  }
                }))
        .apply(
            "Second stateful ParDo",
            ParDo.of(
                new DoFn<KV<String, String>, Void>() {
                  private static final String testState = "testState";

                  @StateId(testState)
                  private final StateSpec<ValueState<Integer>> state = StateSpecs.value();

                  @ProcessElement
                  public void processElement(
                      ProcessContext context, @StateId(testState) ValueState<Integer> state) {}
                }));

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);
    final Set<String> nonUniqueStateIds = StateIdParser.scan(pipeline);
    final ConfigContext configCtx = new ConfigContext(idMap, nonUniqueStateIds, options);
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Config config = configBuilder.build();

    assertEquals(
        RocksDbKeyValueStorageEngineFactory.class.getName(),
        config.get("stores.testState-First_stateful_ParDo.factory"));
    assertEquals("byteArraySerde", config.get("stores.testState-First_stateful_ParDo.key.serde"));
    assertEquals("stateValueSerde", config.get("stores.testState-First_stateful_ParDo.msg.serde"));
    assertNull(config.get("stores.testState-First_stateful_ParDo.changelog"));

    assertEquals(
        RocksDbKeyValueStorageEngineFactory.class.getName(),
        config.get("stores.testState-Second_stateful_ParDo.factory"));
    assertEquals("byteArraySerde", config.get("stores.testState-Second_stateful_ParDo.key.serde"));
    assertEquals("stateValueSerde", config.get("stores.testState-Second_stateful_ParDo.msg.serde"));
    assertNull(config.get("stores.testState-Second_stateful_ParDo.changelog"));

    options.setStateDurable(true);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Config config2 = configBuilder.build();
    assertEquals(
        "TestStoreConfig-1-testState-First_stateful_ParDo-changelog",
        config2.get("stores.testState-First_stateful_ParDo.changelog"));
    assertEquals(
        "TestStoreConfig-1-testState-Second_stateful_ParDo-changelog",
        config2.get("stores.testState-Second_stateful_ParDo.changelog"));
  }

  @Test
  public void testUserStoreConfigSameStateIdAndPTransformName() {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setJobName("TestStoreConfig");
    options.setRunner(SamzaRunner.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            Create.empty(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings())))
        .apply(
            "Same stateful ParDo Name",
            ParDo.of(
                new DoFn<KV<String, String>, KV<String, String>>() {
                  private static final String testState = "testState";

                  @StateId(testState)
                  private final StateSpec<ValueState<Integer>> state = StateSpecs.value();

                  @ProcessElement
                  public void processElement(
                      ProcessContext context, @StateId(testState) ValueState<Integer> state) {
                    context.output(context.element());
                  }
                }))
        .apply(
            "Same stateful ParDo Name",
            ParDo.of(
                new DoFn<KV<String, String>, Void>() {
                  private static final String testState = "testState";

                  @StateId(testState)
                  private final StateSpec<ValueState<Integer>> state = StateSpecs.value();

                  @ProcessElement
                  public void processElement(
                      ProcessContext context, @StateId(testState) ValueState<Integer> state) {}
                }));

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);
    final Set<String> nonUniqueStateIds = StateIdParser.scan(pipeline);
    final ConfigContext configCtx = new ConfigContext(idMap, nonUniqueStateIds, options);

    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Config config = configBuilder.build();

    assertEquals(
        RocksDbKeyValueStorageEngineFactory.class.getName(),
        config.get("stores.testState-Same_stateful_ParDo_Name.factory"));
    assertEquals(
        "byteArraySerde", config.get("stores.testState-Same_stateful_ParDo_Name.key.serde"));
    assertEquals(
        "stateValueSerde", config.get("stores.testState-Same_stateful_ParDo_Name.msg.serde"));
    assertNull(config.get("stores.testState-Same_stateful_ParDo_Name.changelog"));

    assertEquals(
        RocksDbKeyValueStorageEngineFactory.class.getName(),
        config.get("stores.testState-Same_stateful_ParDo_Name2.factory"));
    assertEquals(
        "byteArraySerde", config.get("stores.testState-Same_stateful_ParDo_Name2.key.serde"));
    assertEquals(
        "stateValueSerde", config.get("stores.testState-Same_stateful_ParDo_Name2.msg.serde"));
    assertNull(config.get("stores.testState-Same_stateful_ParDo_Name2.changelog"));

    options.setStateDurable(true);
    SamzaPipelineTranslator.createConfig(pipeline, configCtx, configBuilder);
    final Config config2 = configBuilder.build();
    assertEquals(
        "TestStoreConfig-1-testState-Same_stateful_ParDo_Name-changelog",
        config2.get("stores.testState-Same_stateful_ParDo_Name.changelog"));
    assertEquals(
        "TestStoreConfig-1-testState-Same_stateful_ParDo_Name2-changelog",
        config2.get("stores.testState-Same_stateful_ParDo_Name2.changelog"));
  }

  @Test
  public void testCreateBundleConfig() {
    // autosizing = 0: disabled
    // autosizing = 1: enabled
    for (int autosizing = 0; autosizing < 2; autosizing++) {
      final SamzaPipelineOptions options = PipelineOptionsFactory.as(SamzaPipelineOptions.class);
      final Map<String, String> config = new HashMap<>();

      // bundle size = 1
      options.setMaxBundleSize(1);
      config.put(JobConfig.JOB_CONTAINER_THREAD_POOL_SIZE, "5");
      if (autosizing != 0) {
        // Test autosizing enabled, the output should be the same
        config.put(JobConfig.JOB_AUTOSIZING_CONTAINER_THREAD_POOL_SIZE, "5");
      }

      Map<String, String> bundleConfig = ConfigBuilder.createBundleConfig(options, config);

      assertEquals("1", bundleConfig.get(TaskConfig.MAX_CONCURRENCY));
      assertNull(bundleConfig.get(JobConfig.JOB_CONTAINER_THREAD_POOL_SIZE));
      assertNull(bundleConfig.get(JobConfig.JOB_AUTOSIZING_CONTAINER_THREAD_POOL_SIZE));
      assertEquals(1, options.getNumThreadsForProcessElement());

      // bundle size = 3, NumThreadsForProcessElement = 10
      options.setMaxBundleSize(3);
      options.setNumThreadsForProcessElement(10);
      bundleConfig = ConfigBuilder.createBundleConfig(options, config);

      assertEquals("3", bundleConfig.get(TaskConfig.MAX_CONCURRENCY));
      assertEquals("0", bundleConfig.get(JobConfig.JOB_CONTAINER_THREAD_POOL_SIZE));
      assertEquals("0", bundleConfig.get(JobConfig.JOB_AUTOSIZING_CONTAINER_THREAD_POOL_SIZE));
      assertEquals(10, options.getNumThreadsForProcessElement());

      // bundle size = 3, NumThreadsForProcessElement = 1 (default), threadPoolSize = 5
      options.setNumThreadsForProcessElement(1);
      bundleConfig = ConfigBuilder.createBundleConfig(options, config);

      assertEquals("3", bundleConfig.get(TaskConfig.MAX_CONCURRENCY));
      assertEquals("0", bundleConfig.get(JobConfig.JOB_CONTAINER_THREAD_POOL_SIZE));
      assertEquals("0", bundleConfig.get(JobConfig.JOB_AUTOSIZING_CONTAINER_THREAD_POOL_SIZE));
      assertEquals(5, options.getNumThreadsForProcessElement());
    }
  }
}
