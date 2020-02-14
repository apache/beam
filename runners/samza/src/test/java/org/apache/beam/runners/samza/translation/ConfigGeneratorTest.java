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

import java.util.Map;
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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.job.yarn.YarnJobFactory;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.runtime.RemoteApplicationRunner;
import org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory;
import org.apache.samza.zk.ZkJobCoordinatorFactory;
import org.junit.Test;

/** Test config generations for {@link org.apache.beam.runners.samza.SamzaRunner}. */
public class ConfigGeneratorTest {
  private static final String APP_RUNNER_CLASS = "app.runner.class";
  private static final String JOB_FACTORY_CLASS = "job.factory.class";

  @Test
  public void testBeamStoreConfig() {
    SamzaPipelineOptions options = PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    options.setJobName("TestStoreConfig");
    options.setRunner(SamzaRunner.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Create.of(1, 2, 3)).apply(Sum.integersGlobally());

    pipeline.replaceAll(SamzaTransformOverrides.getDefaultOverrides());

    final Map<PValue, String> idMap = PViewToIdMapper.buildIdMap(pipeline);
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, options, idMap, configBuilder);
    final Config config = configBuilder.build();

    assertEquals(
        RocksDbKeyValueStorageEngineFactory.class.getName(),
        config.get("stores.beamStore.factory"));
    assertEquals("byteArraySerde", config.get("stores.beamStore.key.serde"));
    assertEquals("byteSerde", config.get("stores.beamStore.msg.serde"));
    assertNull(config.get("stores.beamStore.changelog"));

    options.setStateDurable(true);
    SamzaPipelineTranslator.createConfig(pipeline, options, idMap, configBuilder);
    final Config config2 = configBuilder.build();
    assertEquals(
        "TestStoreConfig-1-beamStore-changelog", config2.get("stores.beamStore.changelog"));
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
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, options, idMap, configBuilder);
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
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, options, idMap, configBuilder);
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
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, options, idMap, configBuilder);
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
    final ConfigBuilder configBuilder = new ConfigBuilder(options);
    SamzaPipelineTranslator.createConfig(pipeline, options, idMap, configBuilder);
    final Config config = configBuilder.build();

    assertEquals(
        RocksDbKeyValueStorageEngineFactory.class.getName(),
        config.get("stores.testState.factory"));
    assertEquals("byteArraySerde", config.get("stores.testState.key.serde"));
    assertEquals("byteSerde", config.get("stores.testState.msg.serde"));
    assertNull(config.get("stores.testState.changelog"));

    options.setStateDurable(true);
    SamzaPipelineTranslator.createConfig(pipeline, options, idMap, configBuilder);
    final Config config2 = configBuilder.build();
    assertEquals(
        "TestStoreConfig-1-testState-changelog", config2.get("stores.testState.changelog"));
  }
}
