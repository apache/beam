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

import static java.util.stream.StreamSupport.stream;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import com.google.api.services.dataflow.model.StreamingComputationConfig;
import com.google.api.services.dataflow.model.StreamingConfigTask;
import com.google.api.services.dataflow.model.WorkItem;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public final class StreamingEngineConfigLoader
    implements StreamingConfigLoader<StreamingEnginePipelineConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingEngineConfigLoader.class);
  private static final String GLOBAL_PIPELINE_CONFIG_REFRESHER = "GlobalPipelineConfigRefresher";

  private final long globalConfigRefreshPeriodMillis;
  private final WorkUnitClient dataflowServiceClient;
  private final ScheduledExecutorService globalConfigRefresher;
  private final Consumer<StreamingEnginePipelineConfig> onStreamingConfig;
  private final AtomicBoolean hasReceivedGlobalConfig;

  private StreamingEngineConfigLoader(
      boolean hasReceivedGlobalConfig,
      long globalConfigRefreshPeriodMillis,
      WorkUnitClient dataflowServiceClient,
      ScheduledExecutorService globalConfigRefresher,
      Consumer<StreamingEnginePipelineConfig> onStreamingConfig) {
    this.globalConfigRefreshPeriodMillis = globalConfigRefreshPeriodMillis;
    this.dataflowServiceClient = dataflowServiceClient;
    this.globalConfigRefresher = globalConfigRefresher;
    this.onStreamingConfig = onStreamingConfig;
    this.hasReceivedGlobalConfig = new AtomicBoolean(hasReceivedGlobalConfig);
  }

  public static StreamingEngineConfigLoader create(
      long globalConfigRefreshPeriodMillis,
      WorkUnitClient dataflowServiceClient,
      Consumer<StreamingEnginePipelineConfig> onStreamingConfig) {
    return new StreamingEngineConfigLoader(
        /* hasReceivedGlobalConfig= */ false,
        globalConfigRefreshPeriodMillis,
        dataflowServiceClient,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat(GLOBAL_PIPELINE_CONFIG_REFRESHER).build()),
        onStreamingConfig);
  }

  public static StreamingEngineConfigLoader forTesting(
      boolean hasReceivedGlobalConfig,
      long globalConfigRefreshPeriodMillis,
      WorkUnitClient dataflowServiceClient,
      Function<String, ScheduledExecutorService> executorSupplier,
      Consumer<StreamingEnginePipelineConfig> onStreamingConfig) {
    return new StreamingEngineConfigLoader(
        hasReceivedGlobalConfig,
        globalConfigRefreshPeriodMillis,
        dataflowServiceClient,
        executorSupplier.apply(GLOBAL_PIPELINE_CONFIG_REFRESHER),
        onStreamingConfig);
  }

  private static BackOff defaultConfigBackoff() {
    return FluentBackoff.DEFAULT
        .withInitialBackoff(Duration.millis(100))
        .withMaxBackoff(Duration.standardMinutes(1))
        .withMaxCumulativeBackoff(Duration.standardMinutes(5))
        .backoff();
  }

  @Override
  public void start() {
    fetchInitialPipelineGlobalConfig();
    schedulePeriodicGlobalConfigRequests();
  }

  @Override
  public Optional<StreamingEnginePipelineConfig> getComputationConfig(String computationId) {
    Preconditions.checkArgument(
        !computationId.isEmpty(),
        "computationId is empty. Cannot fetch computation config without a computationId.");
    return getComputationConfigInternal(computationId);
  }

  @Override
  public void stop() {
    // We have already shutdown or start has not been called.
    if (globalConfigRefresher.isShutdown() || !hasReceivedGlobalConfig.get()) {
      return;
    }

    globalConfigRefresher.shutdown();
    boolean isTerminated = false;
    try {
      isTerminated = globalConfigRefresher.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Error occurred shutting down: {}", globalConfigRefresher);
    }
    if (!isTerminated) {
      globalConfigRefresher.shutdownNow();
    }
  }

  /**
   * Initially b Schedules a background thread that periodically sends getConfig requests to
   * Dataflow Service to obtain the windmill service endpoint.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void schedulePeriodicGlobalConfigRequests() {
    globalConfigRefresher.scheduleWithFixedDelay(
        () -> getGlobalConfig().ifPresent(onStreamingConfig),
        0,
        globalConfigRefreshPeriodMillis,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Blocks until we have received the initial global config from Dataflow or an exception is
   * thrown.
   */
  private synchronized void fetchInitialPipelineGlobalConfig() {
    if (!hasReceivedGlobalConfig.get()) {
      // Get the initial global configuration. This will initialize the windmillServer stub.
      while (true) {
        LOG.info("Sending request to get initial global configuration for this worker.");
        Optional<StreamingEnginePipelineConfig> globalConfig = getGlobalConfig();
        if (globalConfig.isPresent()) {
          onStreamingConfig.accept(globalConfig.get());
          hasReceivedGlobalConfig.compareAndSet(false, true);
          break;
        }
        LOG.info("Haven't received initial global configuration, will retry in 5 seconds");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }
    }
    LOG.info("Initial global configuration received, harness is now ready");
  }

  private Optional<StreamingEnginePipelineConfig> getComputationConfigInternal(
      String computationId) {
    Optional<StreamingEnginePipelineConfig> streamingConfig = getConfig(computationId);
    streamingConfig.ifPresent(onStreamingConfig);
    return streamingConfig;
  }

  private Optional<StreamingEnginePipelineConfig> getGlobalConfig() {
    return getConfig(null);
  }

  private Optional<StreamingEnginePipelineConfig> getConfig(@Nullable String computation) {
    BackOff backoff = defaultConfigBackoff();
    while (true) {
      try {
        return getConfigFromDataflowService(computation);
      } catch (IllegalArgumentException | IOException e) {
        LOG.warn("Error fetching config: ", e);
        try {
          if (!BackOffUtils.next(Sleeper.DEFAULT, backoff)) {
            return Optional.empty();
          }
        } catch (IOException ioe) {
          LOG.warn("Error backing off, will not retry: ", ioe);
          return Optional.empty();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return Optional.empty();
        }
      }
    }
  }

  /**
   * Sends a request to get pipeline configuration from Dataflow, either for a specific computation
   * (if computation is not null) or global configuration (if computation is null).
   *
   * @throws IOException if the RPC fails.
   */
  private Optional<StreamingEnginePipelineConfig> getConfigFromDataflowService(
      @Nullable String computation) throws IOException {
    Optional<WorkItem> workItem =
        computation != null
            ? dataflowServiceClient.getStreamingConfigWorkItem(computation)
            : dataflowServiceClient.getGlobalStreamingConfigWorkItem();

    if (!workItem.isPresent()) {
      return Optional.empty();
    }

    StreamingConfigTask config = workItem.get().getStreamingConfigTask();
    Preconditions.checkState(config != null);

    StreamingEnginePipelineConfig.Builder streamingConfig = StreamingEnginePipelineConfig.builder();

    if (config.getUserStepToStateFamilyNameMap() != null) {
      streamingConfig.setUserStepToStateFamilyNameMap(config.getUserStepToStateFamilyNameMap());
    }

    // Computation is null for requests to fetch global pipeline config.
    if (computation == null) {
      if (config.getMaxWorkItemCommitBytes() != null
          && config.getMaxWorkItemCommitBytes() > 0
          && config.getMaxWorkItemCommitBytes() <= Integer.MAX_VALUE) {
        streamingConfig.setMaxWorkItemCommitBytes(config.getMaxWorkItemCommitBytes().intValue());
      }
    }

    List<StreamingComputationConfig> configs = config.getStreamingComputationConfigs();
    if (configs != null) {
      streamingConfig.setComputationConfigs(ImmutableList.copyOf(configs));
    }

    if (config.getWindmillServiceEndpoint() != null
        && !config.getWindmillServiceEndpoint().isEmpty()) {
      int port =
          config.getWindmillServicePort() != null && config.getWindmillServicePort() != 0
              ? config.getWindmillServicePort().intValue()
              : 443;

      ImmutableSet<HostAndPort> endpoints =
          stream(
                  Splitter.on(',').split(config.getWindmillServiceEndpoint()).spliterator(),
                  /* isParallel= */ false)
              .map(endpoint -> HostAndPort.fromString(endpoint).withDefaultPort(port))
              .collect(toImmutableSet());

      streamingConfig.setWindmillServiceEndpoints(endpoints);
    }

    return Optional.of(streamingConfig.build());
  }
}
