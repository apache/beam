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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.StreamingComputationConfig;
import com.google.api.services.dataflow.model.StreamingConfigTask;
import com.google.api.services.dataflow.model.WorkItem;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.apache.beam.runners.dataflow.worker.OperationalLimits;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.internal.stubbing.answers.Returns;

@RunWith(JUnit4.class)
public class StreamingEngineComputationConfigFetcherTest {

  private final WorkUnitClient mockDataflowServiceClient =
      mock(WorkUnitClient.class, new Returns(Optional.empty()));
  private StreamingEngineComputationConfigFetcher streamingEngineConfigFetcher;

  private StreamingEngineComputationConfigFetcher createConfigFetcher(
      boolean waitForInitialConfig,
      long globalConfigRefreshPeriod,
      StreamingGlobalConfigHandleImpl globalConfigHandle) {
    return StreamingEngineComputationConfigFetcher.forTesting(
        !waitForInitialConfig,
        globalConfigRefreshPeriod,
        mockDataflowServiceClient,
        globalConfigHandle,
        ignored -> Executors.newSingleThreadScheduledExecutor());
  }

  @After
  public void cleanUp() {
    streamingEngineConfigFetcher.stop();
  }

  @Test
  public void testStart_requiresInitialConfig() throws IOException, InterruptedException {
    WorkItem initialConfig =
        new WorkItem()
            .setJobId("job")
            .setStreamingConfigTask(new StreamingConfigTask().setMaxWorkItemCommitBytes(10L));
    CountDownLatch waitForInitialConfig = new CountDownLatch(1);
    Set<StreamingGlobalConfig> receivedPipelineConfig = new HashSet<>();
    when(mockDataflowServiceClient.getGlobalStreamingConfigWorkItem())
        .thenReturn(Optional.of(initialConfig));
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    globalConfigHandle.registerConfigObserver(
        config -> {
          try {
            receivedPipelineConfig.add(config);
            waitForInitialConfig.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
    streamingEngineConfigFetcher =
        createConfigFetcher(/* waitForInitialConfig= */ true, 0, globalConfigHandle);
    Thread asyncStartConfigLoader = new Thread(streamingEngineConfigFetcher::start);
    asyncStartConfigLoader.start();
    waitForInitialConfig.countDown();
    asyncStartConfigLoader.join();
    StreamingGlobalConfig.Builder configBuilder =
        StreamingGlobalConfig.builder()
            .setOperationalLimits(
                OperationalLimits.builder()
                    .setMaxWorkItemCommitBytes(
                        initialConfig.getStreamingConfigTask().getMaxWorkItemCommitBytes())
                    .build());
    assertThat(receivedPipelineConfig).containsExactly(configBuilder.build());
  }

  @Test
  public void testStart_startsPeriodicConfigRequests() throws IOException, InterruptedException {
    WorkItem firstConfig =
        new WorkItem()
            .setJobId("job")
            .setStreamingConfigTask(new StreamingConfigTask().setMaxWorkItemCommitBytes(10L));
    WorkItem secondConfig =
        new WorkItem()
            .setJobId("job")
            .setStreamingConfigTask(new StreamingConfigTask().setMaxWorkItemCommitBytes(15L));
    WorkItem thirdConfig =
        new WorkItem()
            .setJobId("job")
            .setStreamingConfigTask(new StreamingConfigTask().setMaxWorkItemCommitBytes(100L));
    CountDownLatch numExpectedRefreshes = new CountDownLatch(3);
    Set<StreamingGlobalConfig> receivedPipelineConfig = new HashSet<>();
    when(mockDataflowServiceClient.getGlobalStreamingConfigWorkItem())
        .thenReturn(Optional.of(firstConfig))
        .thenReturn(Optional.of(secondConfig))
        // ConfigFetcher should still fetch subsequent configs on error.
        .thenThrow(new IOException("something bad happened."))
        .thenReturn(Optional.of(thirdConfig))
        // ConfigFetcher should not do anything with a config that doesn't contain a
        // StreamingConfigTask.
        .thenReturn(Optional.of(new WorkItem().setJobId("jobId")));
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    globalConfigHandle.registerConfigObserver(
        config -> {
          receivedPipelineConfig.add(config);
          numExpectedRefreshes.countDown();
        });
    streamingEngineConfigFetcher =
        createConfigFetcher(
            /* waitForInitialConfig= */ true, Duration.millis(100).getMillis(), globalConfigHandle);

    Thread asyncStartConfigLoader = new Thread(streamingEngineConfigFetcher::start);
    asyncStartConfigLoader.start();
    numExpectedRefreshes.await();
    asyncStartConfigLoader.join();
    assertThat(receivedPipelineConfig)
        .containsExactly(
            StreamingGlobalConfig.builder()
                .setOperationalLimits(
                    OperationalLimits.builder()
                        .setMaxWorkItemCommitBytes(
                            firstConfig.getStreamingConfigTask().getMaxWorkItemCommitBytes())
                        .build())
                .build(),
            StreamingGlobalConfig.builder()
                .setOperationalLimits(
                    OperationalLimits.builder()
                        .setMaxWorkItemCommitBytes(
                            secondConfig.getStreamingConfigTask().getMaxWorkItemCommitBytes())
                        .build())
                .build(),
            StreamingGlobalConfig.builder()
                .setOperationalLimits(
                    OperationalLimits.builder()
                        .setMaxWorkItemCommitBytes(
                            thirdConfig.getStreamingConfigTask().getMaxWorkItemCommitBytes())
                        .build())
                .build());
  }

  @Test
  public void testGetComputationConfig() throws IOException {
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    streamingEngineConfigFetcher =
        createConfigFetcher(/* waitForInitialConfig= */ false, 0, globalConfigHandle);
    String computationId = "computationId";
    String stageName = "stageName";
    String systemName = "systemName";
    StreamingComputationConfig pipelineConfig =
        new StreamingComputationConfig()
            .setComputationId(computationId)
            .setStageName(stageName)
            .setSystemName(systemName)
            .setInstructions(ImmutableList.of());

    WorkItem workItem =
        new WorkItem()
            .setStreamingConfigTask(
                new StreamingConfigTask()
                    .setStreamingComputationConfigs(ImmutableList.of(pipelineConfig)));

    when(mockDataflowServiceClient.getStreamingConfigWorkItem(anyString()))
        .thenReturn(Optional.of(workItem));
    Optional<ComputationConfig> actualPipelineConfig =
        streamingEngineConfigFetcher.fetchConfig(computationId);

    assertTrue(actualPipelineConfig.isPresent());
    assertThat(actualPipelineConfig.get())
        .isEqualTo(
            ComputationConfig.create(
                StreamingEngineComputationConfigFetcher.createMapTask(pipelineConfig),
                ImmutableMap.of(),
                ImmutableMap.of()));
  }

  @Test
  public void testGetComputationConfig_noComputationPresent() throws IOException {
    Set<StreamingGlobalConfig> receivedPipelineConfig = new HashSet<>();
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    globalConfigHandle.registerConfigObserver(receivedPipelineConfig::add);
    streamingEngineConfigFetcher =
        createConfigFetcher(/* waitForInitialConfig= */ false, 0, globalConfigHandle);
    when(mockDataflowServiceClient.getStreamingConfigWorkItem(anyString()))
        .thenReturn(Optional.empty());
    Optional<ComputationConfig> pipelineConfig =
        streamingEngineConfigFetcher.fetchConfig("someComputationId");
    assertFalse(pipelineConfig.isPresent());
    assertThat(receivedPipelineConfig).isEmpty();
  }

  @Test
  public void testGetComputationConfig_fetchConfigFromDataflowError() throws IOException {
    StreamingGlobalConfigHandleImpl globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    streamingEngineConfigFetcher =
        createConfigFetcher(/* waitForInitialConfig= */ false, 0, globalConfigHandle);
    RuntimeException e = new RuntimeException("something bad happened.");
    when(mockDataflowServiceClient.getStreamingConfigWorkItem(anyString())).thenThrow(e);
    Throwable fetchConfigError =
        assertThrows(
            RuntimeException.class,
            () -> streamingEngineConfigFetcher.fetchConfig("someComputationId"));
    assertThat(fetchConfigError).isSameInstanceAs(e);
  }
}
