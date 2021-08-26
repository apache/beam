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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Collections;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.RemoteEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

/** Tests for {@link FlinkExecutionEnvironments}. */
public class FlinkExecutionEnvironmentsTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldSetParallelismBatch() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(TestFlinkRunner.class);
    options.setParallelism(42);

    ExecutionEnvironment bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(options.getParallelism(), is(42));
    assertThat(bev.getParallelism(), is(42));
  }

  @Test
  public void shouldSetParallelismStreaming() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(TestFlinkRunner.class);
    options.setParallelism(42);

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(options.getParallelism(), is(42));
    assertThat(sev.getParallelism(), is(42));
  }

  @Test
  public void shouldSetMaxParallelismStreaming() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(TestFlinkRunner.class);
    options.setMaxParallelism(42);

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(options.getMaxParallelism(), is(42));
    assertThat(sev.getMaxParallelism(), is(42));
  }

  @Test
  public void shouldInferParallelismFromEnvironmentBatch() throws IOException {
    String flinkConfDir = extractFlinkConfig();

    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(TestFlinkRunner.class);
    options.setFlinkMaster("host:80");

    ExecutionEnvironment bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList(), flinkConfDir);

    assertThat(options.getParallelism(), is(23));
    assertThat(bev.getParallelism(), is(23));
  }

  @Test
  public void shouldInferParallelismFromEnvironmentStreaming() throws IOException {
    String confDir = extractFlinkConfig();

    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(TestFlinkRunner.class);
    options.setFlinkMaster("host:80");

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList(), confDir);

    assertThat(options.getParallelism(), is(23));
    assertThat(sev.getParallelism(), is(23));
  }

  @Test
  public void shouldFallbackToDefaultParallelismBatch() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(TestFlinkRunner.class);
    options.setFlinkMaster("host:80");

    ExecutionEnvironment bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(options.getParallelism(), is(1));
    assertThat(bev.getParallelism(), is(1));
  }

  @Test
  public void shouldFallbackToDefaultParallelismStreaming() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(TestFlinkRunner.class);
    options.setFlinkMaster("host:80");

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(options.getParallelism(), is(1));
    assertThat(sev.getParallelism(), is(1));
  }

  @Test
  public void useDefaultParallelismFromContextBatch() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(TestFlinkRunner.class);

    ExecutionEnvironment bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(bev, instanceOf(LocalEnvironment.class));
    assertThat(options.getParallelism(), is(LocalStreamEnvironment.getDefaultLocalParallelism()));
    assertThat(bev.getParallelism(), is(LocalStreamEnvironment.getDefaultLocalParallelism()));
  }

  @Test
  public void useDefaultParallelismFromContextStreaming() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(TestFlinkRunner.class);

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(sev, instanceOf(LocalStreamEnvironment.class));
    assertThat(options.getParallelism(), is(LocalStreamEnvironment.getDefaultLocalParallelism()));
    assertThat(sev.getParallelism(), is(LocalStreamEnvironment.getDefaultLocalParallelism()));
  }

  @Test
  public void shouldParsePortForRemoteEnvironmentBatch() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);
    options.setFlinkMaster("host:1234");

    ExecutionEnvironment bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(bev, instanceOf(RemoteEnvironment.class));
    checkHostAndPort(bev, "host", 1234);
  }

  @Test
  public void shouldParsePortForRemoteEnvironmentStreaming() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);
    options.setFlinkMaster("host:1234");

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(sev, instanceOf(RemoteStreamEnvironment.class));
    checkHostAndPort(sev, "host", 1234);
  }

  @Test
  public void shouldAllowPortOmissionForRemoteEnvironmentBatch() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);
    options.setFlinkMaster("host");

    ExecutionEnvironment bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(bev, instanceOf(RemoteEnvironment.class));
    checkHostAndPort(bev, "host", RestOptions.PORT.defaultValue());
  }

  @Test
  public void shouldAllowPortOmissionForRemoteEnvironmentStreaming() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);
    options.setFlinkMaster("host");

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(sev, instanceOf(RemoteStreamEnvironment.class));
    checkHostAndPort(sev, "host", RestOptions.PORT.defaultValue());
  }

  @Test
  public void shouldTreatAutoAndEmptyHostTheSameBatch() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);

    ExecutionEnvironment sev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());

    options.setFlinkMaster("[auto]");

    ExecutionEnvironment sev2 =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());

    assertEquals(sev.getClass(), sev2.getClass());
  }

  @Test
  public void shouldTreatAutoAndEmptyHostTheSameStreaming() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    options.setFlinkMaster("[auto]");

    StreamExecutionEnvironment sev2 =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    assertEquals(sev.getClass(), sev2.getClass());
  }

  @Test
  public void shouldDetectMalformedPortBatch() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);
    options.setFlinkMaster("host:p0rt");

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unparseable port number");

    FlinkExecutionEnvironments.createBatchExecutionEnvironment(options, Collections.emptyList());
  }

  @Test
  public void shouldDetectMalformedPortStreaming() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);
    options.setFlinkMaster("host:p0rt");

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unparseable port number");

    FlinkExecutionEnvironments.createStreamExecutionEnvironment(options, Collections.emptyList());
  }

  @Test
  public void shouldSupportIPv4Batch() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);

    options.setFlinkMaster("192.168.1.1:1234");
    ExecutionEnvironment bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());
    checkHostAndPort(bev, "192.168.1.1", 1234);

    options.setFlinkMaster("192.168.1.1");
    bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());
    checkHostAndPort(bev, "192.168.1.1", RestOptions.PORT.defaultValue());
  }

  @Test
  public void shouldSupportIPv4Streaming() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);

    options.setFlinkMaster("192.168.1.1:1234");
    ExecutionEnvironment bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());
    checkHostAndPort(bev, "192.168.1.1", 1234);

    options.setFlinkMaster("192.168.1.1");
    bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());
    checkHostAndPort(bev, "192.168.1.1", RestOptions.PORT.defaultValue());
  }

  @Test
  public void shouldSupportIPv6Batch() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);

    options.setFlinkMaster("[FE80:CD00:0000:0CDE:1257:0000:211E:729C]:1234");
    ExecutionEnvironment bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());
    checkHostAndPort(bev, "FE80:CD00:0000:0CDE:1257:0000:211E:729C", 1234);

    options.setFlinkMaster("FE80:CD00:0000:0CDE:1257:0000:211E:729C");
    bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());
    checkHostAndPort(
        bev, "FE80:CD00:0000:0CDE:1257:0000:211E:729C", RestOptions.PORT.defaultValue());
  }

  @Test
  public void shouldSupportIPv6Streaming() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);

    options.setFlinkMaster("[FE80:CD00:0000:0CDE:1257:0000:211E:729C]:1234");
    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());
    checkHostAndPort(sev, "FE80:CD00:0000:0CDE:1257:0000:211E:729C", 1234);

    options.setFlinkMaster("FE80:CD00:0000:0CDE:1257:0000:211E:729C");
    sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());
    checkHostAndPort(
        sev, "FE80:CD00:0000:0CDE:1257:0000:211E:729C", RestOptions.PORT.defaultValue());
  }

  @Test
  public void shouldRemoveHttpProtocolFromHostBatch() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);

    for (String flinkMaster :
        new String[] {
          "http://host:1234", " http://host:1234", "https://host:1234", " https://host:1234"
        }) {
      options.setFlinkMaster(flinkMaster);
      ExecutionEnvironment sev =
          FlinkExecutionEnvironments.createBatchExecutionEnvironment(
              options, Collections.emptyList());
      checkHostAndPort(sev, "host", 1234);
    }
  }

  @Test
  public void shouldRemoveHttpProtocolFromHostStreaming() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);

    for (String flinkMaster :
        new String[] {
          "http://host:1234", " http://host:1234", "https://host:1234", " https://host:1234"
        }) {
      options.setFlinkMaster(flinkMaster);
      StreamExecutionEnvironment sev =
          FlinkExecutionEnvironments.createStreamExecutionEnvironment(
              options, Collections.emptyList());
      checkHostAndPort(sev, "host", 1234);
    }
  }

  private String extractFlinkConfig() throws IOException {
    InputStream inputStream = getClass().getResourceAsStream("/flink-conf.yaml");
    File root = temporaryFolder.getRoot();
    Files.copy(inputStream, new File(root, "flink-conf.yaml").toPath());
    return root.getAbsolutePath();
  }

  @Test
  public void shouldAutoSetIdleSourcesFlagWithoutCheckpointing() {
    // Checkpointing disabled, shut down sources immediately
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    FlinkExecutionEnvironments.createStreamExecutionEnvironment(options, Collections.emptyList());
    assertThat(options.getShutdownSourcesAfterIdleMs(), is(0L));
  }

  @Test
  public void shouldAutoSetIdleSourcesFlagWithCheckpointing() {
    // Checkpointing is enabled, never shut down sources
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setCheckpointingInterval(1000L);
    FlinkExecutionEnvironments.createStreamExecutionEnvironment(options, Collections.emptyList());
    assertThat(options.getShutdownSourcesAfterIdleMs(), is(Long.MAX_VALUE));
  }

  @Test
  public void shouldAcceptExplicitlySetIdleSourcesFlagWithoutCheckpointing() {
    // Checkpointing disabled, accept flag
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setShutdownSourcesAfterIdleMs(42L);
    FlinkExecutionEnvironments.createStreamExecutionEnvironment(options, Collections.emptyList());
    assertThat(options.getShutdownSourcesAfterIdleMs(), is(42L));
  }

  @Test
  public void shouldAcceptExplicitlySetIdleSourcesFlagWithCheckpointing() {
    // Checkpointing enable, still accept flag
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setCheckpointingInterval(1000L);
    options.setShutdownSourcesAfterIdleMs(42L);
    FlinkExecutionEnvironments.createStreamExecutionEnvironment(options, Collections.emptyList());
    assertThat(options.getShutdownSourcesAfterIdleMs(), is(42L));
  }

  @Test
  public void shouldSetSavepointRestoreForRemoteStreaming() {
    String path = "fakePath";
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(TestFlinkRunner.class);
    options.setFlinkMaster("host:80");
    options.setSavepointPath(path);

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());
    // subject to change with https://issues.apache.org/jira/browse/FLINK-11048
    assertThat(sev, instanceOf(RemoteStreamEnvironment.class));
    assertThat(getSavepointPath(sev), is(path));
  }

  @Test
  public void shouldFailOnUnknownStateBackend() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setStreaming(true);
    options.setStateBackend("unknown");
    options.setStateBackendStoragePath("/path");

    assertThrows(
        "State backend was set to 'unknown' but no storage path was provided.",
        IllegalArgumentException.class,
        () ->
            FlinkExecutionEnvironments.createStreamExecutionEnvironment(
                options, Collections.emptyList()));
  }

  @Test
  public void shouldFailOnNoStoragePathProvided() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setStreaming(true);
    options.setStateBackend("unknown");

    assertThrows(
        "State backend was set to 'unknown' but no storage path was provided.",
        IllegalArgumentException.class,
        () ->
            FlinkExecutionEnvironments.createStreamExecutionEnvironment(
                options, Collections.emptyList()));
  }

  @Test
  public void shouldCreateFileSystemStateBackend() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setStreaming(true);
    options.setStateBackend("fileSystem");
    options.setStateBackendStoragePath(temporaryFolder.getRoot().toURI().toString());

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(sev.getStateBackend(), instanceOf(FsStateBackend.class));
  }

  @Test
  public void shouldCreateRocksDbStateBackend() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setStreaming(true);
    options.setStateBackend("rocksDB");
    options.setStateBackendStoragePath(temporaryFolder.getRoot().toURI().toString());

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(sev.getStateBackend(), instanceOf(RocksDBStateBackend.class));
  }

  private void checkHostAndPort(Object env, String expectedHost, int expectedPort) {
    String host =
        ((Configuration) Whitebox.getInternalState(env, "configuration"))
            .getString(RestOptions.ADDRESS);
    int port =
        ((Configuration) Whitebox.getInternalState(env, "configuration"))
            .getInteger(RestOptions.PORT);
    assertThat(
        new InetSocketAddress(host, port), is(new InetSocketAddress(expectedHost, expectedPort)));
  }

  private String getSavepointPath(Object env) {
    return ((Configuration) Whitebox.getInternalState(env, "configuration"))
        .getString("execution.savepoint.path", null);
  }
}
