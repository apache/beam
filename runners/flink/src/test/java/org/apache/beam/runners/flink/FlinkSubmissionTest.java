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
package org.apache.beam.runners.flink;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.Permission;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.construction.resources.PipelineResources;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End-to-end submission test of Beam jobs on a Flink cluster. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class FlinkSubmissionTest {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkSubmissionTest.class);

  @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Map<String, String> ENV = System.getenv();
  private static final SecurityManager SECURITY_MANAGER = System.getSecurityManager();

  /** Flink cluster that runs over the lifespan of the tests. */
  private static transient RemoteMiniCluster flinkCluster;

  /** Each test has a timeout of 60 seconds (for safety). */
  @Rule public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

  /** Counter which keeps track of the number of jobs submitted. */
  private static int expectedNumberOfJobs;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration config = new Configuration();
    // Avoid port collision in parallel tests on the same machine
    config.setInteger(RestOptions.PORT.key(), 0);

    MiniClusterConfiguration clusterConfig =
        new MiniClusterConfiguration.Builder()
            .setConfiguration(config)
            .setNumTaskManagers(1)
            .setNumSlotsPerTaskManager(1)
            // Create a shared actor system for all cluster services
            .setRpcServiceSharing(RpcServiceSharing.SHARED)
            .build();

    flinkCluster = new RemoteMiniClusterImpl(clusterConfig);
    flinkCluster.start();
    prepareEnvironment();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    restoreEnvironment();
    flinkCluster.close();
    flinkCluster = null;
  }

  @Test
  public void testSubmissionBatch() throws Exception {
    runSubmission(false, false, false);
  }

  @Test
  public void testSubmissionBatchUseDataStream() throws Exception {
    runSubmission(false, false, true);
  }

  @Test
  public void testSubmissionStreaming() throws Exception {
    runSubmission(false, true, false);
  }

  @Test
  public void testDetachedSubmissionBatch() throws Exception {
    runSubmission(true, false, false);
  }

  @Test
  public void testDetachedSubmissionBatchUseDataStream() throws Exception {
    runSubmission(true, false, true);
  }

  @Test
  public void testDetachedSubmissionStreaming() throws Exception {
    runSubmission(true, true, false);
  }

  private void runSubmission(boolean isDetached, boolean isStreaming, boolean useDataStreamForBatch)
      throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.as(FlinkPipelineOptions.class).setStreaming(isStreaming);
    options.setTempLocation(TEMP_FOLDER.getRoot().getPath());
    String jarPath =
        Iterables.getFirst(
            PipelineResources.detectClassPathResourcesToStage(getClass().getClassLoader(), options),
            null);

    try {
      throwExceptionOnSystemExit();
      ImmutableList.Builder<String> argsBuilder = ImmutableList.builder();
      argsBuilder.add("run").add("-c").add(getClass().getName());
      if (isDetached) {
        argsBuilder.add("-d");
      }
      argsBuilder.add(jarPath);
      argsBuilder.add("--runner=flink");

      if (isStreaming) {
        argsBuilder.add("--streaming");
      }

      if (useDataStreamForBatch) {
        argsBuilder.add("--useDataStreamForBatch");
      }

      FlinkSubmissionTest.expectedNumberOfJobs++;
      // Run end-to-end test
      CliFrontend.main(argsBuilder.build().toArray(new String[0]));
    } catch (SystemExitException e) {
      // The CliFrontend exited and we can move on to check if the job has finished
    } finally {
      restoreDefaultSystemExitBehavior();
    }

    waitUntilJobIsCompleted();
  }

  private void waitUntilJobIsCompleted() throws Exception {
    while (true) {
      Collection<JobStatusMessage> allJobsStates = flinkCluster.listJobs().get();
      if (allJobsStates.size() == expectedNumberOfJobs
          && allJobsStates.stream()
              .allMatch(jobStatus -> jobStatus.getJobState().isTerminalState())) {
        LOG.info(
            "All job finished with statuses: {}",
            allJobsStates.stream().map(j -> j.getJobState().name()).collect(Collectors.toList()));
        return;
      }
      Thread.sleep(50);
    }
  }

  /** The Flink program which is executed by the CliFrontend. */
  public static void main(String[] args) {
    FlinkPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    options.setParallelism(1);
    Pipeline p = Pipeline.create(options);
    p.apply(GenerateSequence.from(0).to(1));
    p.run();
  }

  private static void prepareEnvironment() throws Exception {
    // Write a Flink config
    File file = TEMP_FOLDER.newFile("flink-conf.yaml");
    String config =
        String.format(
            "%s: %s\n%s: %s\n%s: %s",
            JobManagerOptions.ADDRESS.key(),
            "localhost",
            JobManagerOptions.PORT.key(),
            flinkCluster.getClusterPort(),
            RestOptions.PORT.key(),
            flinkCluster.getRestPort());

    Files.write(file.toPath(), config.getBytes(StandardCharsets.UTF_8));

    // Create a new environment with the location of the Flink config for CliFrontend
    ImmutableMap<String, String> newEnv =
        ImmutableMap.<String, String>builder()
            .putAll(ENV.entrySet())
            .put(ConfigConstants.ENV_FLINK_CONF_DIR, file.getParent())
            .build();

    modifyEnv(newEnv);
  }

  private static void restoreEnvironment() throws Exception {
    modifyEnv(ENV);
  }

  /**
   * We modify the JVM's environment variables here. This is necessary for the end-to-end test
   * because Flink's CliFrontend requires a Flink configuration file for which the location can only
   * be set using the {@code ConfigConstants.ENV_FLINK_CONF_DIR} environment variable.
   */
  private static void modifyEnv(Map<String, String> env) throws Exception {
    Class processEnv = Class.forName("java.lang.ProcessEnvironment");
    Field envField = processEnv.getDeclaredField("theUnmodifiableEnvironment");

    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(envField, envField.getModifiers() & ~Modifier.FINAL);

    envField.setAccessible(true);
    envField.set(null, env);
    envField.setAccessible(false);

    modifiersField.setInt(envField, envField.getModifiers() & Modifier.FINAL);
    modifiersField.setAccessible(false);
  }

  /** Prevents the CliFrontend from calling System.exit. */
  private static void throwExceptionOnSystemExit() {
    System.setSecurityManager(
        new SecurityManager() {
          @Override
          public void checkPermission(Permission permission) {
            if (permission.getName().startsWith("exitVM")) {
              throw new SystemExitException();
            }
            if (SECURITY_MANAGER != null) {
              SECURITY_MANAGER.checkPermission(permission);
            }
          }
        });
  }

  private static void restoreDefaultSystemExitBehavior() {
    System.setSecurityManager(SECURITY_MANAGER);
  }

  private static class SystemExitException extends SecurityException {}
}
