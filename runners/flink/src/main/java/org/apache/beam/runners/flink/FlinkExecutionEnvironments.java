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

import com.google.common.annotations.VisibleForTesting;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for Flink execution environments. */
public class FlinkExecutionEnvironments {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkExecutionEnvironments.class);

  /**
   * If the submitted job is a batch processing job, this method creates the adequate Flink {@link
   * org.apache.flink.api.java.ExecutionEnvironment} depending on the user-specified options.
   */
  public static ExecutionEnvironment createBatchExecutionEnvironment(
      FlinkPipelineOptions options, List<String> filesToStage) {
    return createBatchExecutionEnvironment(options, filesToStage, null);
  }

  static ExecutionEnvironment createBatchExecutionEnvironment(
      FlinkPipelineOptions options, List<String> filesToStage, @Nullable String confDir) {

    LOG.info("Creating a Batch Execution Environment.");

    String masterUrl = options.getFlinkMaster();
    Configuration flinkConfiguration = getFlinkConfiguration(confDir);
    ExecutionEnvironment flinkBatchEnv;

    // depending on the master, create the right environment.
    if ("[local]".equals(masterUrl)) {
      flinkBatchEnv = ExecutionEnvironment.createLocalEnvironment(flinkConfiguration);
    } else if ("[collection]".equals(masterUrl)) {
      flinkBatchEnv = new CollectionEnvironment();
    } else if ("[auto]".equals(masterUrl)) {
      flinkBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
    } else {
      String[] hostAndPort = masterUrl.split(":", 2);
      final String host = hostAndPort[0];
      final int port;
      if (hostAndPort.length > 1) {
        try {
          port = Integer.parseInt(hostAndPort[1]);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Provided port is malformed: " + hostAndPort[1]);
        }
        flinkConfiguration.setInteger(RestOptions.PORT, port);
      } else {
        port = flinkConfiguration.getInteger(RestOptions.PORT);
      }
      flinkBatchEnv =
          ExecutionEnvironment.createRemoteEnvironment(
              host,
              port,
              flinkConfiguration,
              filesToStage.toArray(new String[filesToStage.size()]));
      LOG.info("Using Flink Master URL {}:{}.", host, port);
    }

    // Set the execution more for data exchange.
    flinkBatchEnv.getConfig().setExecutionMode(options.getExecutionModeForBatch());

    // set the correct parallelism.
    if (options.getParallelism() != -1 && !(flinkBatchEnv instanceof CollectionEnvironment)) {
      flinkBatchEnv.setParallelism(options.getParallelism());
    }
    // Set the correct parallelism, required by UnboundedSourceWrapper to generate consistent splits.
    final int parallelism;
    if (flinkBatchEnv instanceof CollectionEnvironment) {
      parallelism = 1;
    } else {
      parallelism =
          determineParallelism(
              options.getParallelism(), flinkBatchEnv.getParallelism(), flinkConfiguration);
    }

    flinkBatchEnv.setParallelism(parallelism);
    // set parallelism in the options (required by some execution code)
    options.setParallelism(parallelism);

    if (options.getObjectReuse()) {
      flinkBatchEnv.getConfig().enableObjectReuse();
    } else {
      flinkBatchEnv.getConfig().disableObjectReuse();
    }

    applyLatencyTrackingInterval(flinkBatchEnv.getConfig(), options);

    return flinkBatchEnv;
  }

  /**
   * If the submitted job is a stream processing job, this method creates the adequate Flink {@link
   * org.apache.flink.streaming.api.environment.StreamExecutionEnvironment} depending on the
   * user-specified options.
   */
  public static StreamExecutionEnvironment createStreamExecutionEnvironment(
      FlinkPipelineOptions options, List<String> filesToStage) {
    return createStreamExecutionEnvironment(options, filesToStage, null);
  }

  @VisibleForTesting
  static StreamExecutionEnvironment createStreamExecutionEnvironment(
      FlinkPipelineOptions options, List<String> filesToStage, @Nullable String confDir) {

    LOG.info("Creating a Streaming Environment.");

    String masterUrl = options.getFlinkMaster();
    Configuration flinkConfig = getFlinkConfiguration(confDir);
    final StreamExecutionEnvironment flinkStreamEnv;

    // depending on the master, create the right environment.
    if ("[local]".equals(masterUrl)) {
      flinkStreamEnv = StreamExecutionEnvironment.createLocalEnvironment();
    } else if ("[auto]".equals(masterUrl)) {
      flinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    } else {
      String[] hostAndPort = masterUrl.split(":", 2);
      final String host = hostAndPort[0];
      final int port;
      if (hostAndPort.length > 1) {
        try {
          port = Integer.parseInt(hostAndPort[1]);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Provided port is malformed: " + hostAndPort[1]);
        }
        flinkConfig.setInteger(RestOptions.PORT, port);
      } else {
        port = flinkConfig.getInteger(RestOptions.PORT);
      }
      final SavepointRestoreSettings savepointRestoreSettings;
      if (options.getSavepointPath() != null) {
        savepointRestoreSettings =
            SavepointRestoreSettings.forPath(
                options.getSavepointPath(), options.getAllowNonRestoredState());
      } else {
        savepointRestoreSettings = SavepointRestoreSettings.none();
      }
      flinkStreamEnv =
          new BeamFlinkRemoteStreamEnvironment(
              host,
              port,
              flinkConfig,
              savepointRestoreSettings,
              filesToStage.toArray(new String[filesToStage.size()]));
      LOG.info("Using Flink Master URL {}:{}.", host, port);
    }

    // Set the parallelism, required by UnboundedSourceWrapper to generate consistent splits.
    final int parallelism =
        determineParallelism(
            options.getParallelism(), flinkStreamEnv.getParallelism(), flinkConfig);
    flinkStreamEnv.setParallelism(parallelism);
    // set parallelism in the options (required by some execution code)
    options.setParallelism(parallelism);

    if (options.getObjectReuse()) {
      flinkStreamEnv.getConfig().enableObjectReuse();
    } else {
      flinkStreamEnv.getConfig().disableObjectReuse();
    }

    // default to event time
    flinkStreamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // for the following 2 parameters, a value of -1 means that Flink will use
    // the default values as specified in the configuration.
    int numRetries = options.getNumberOfExecutionRetries();
    if (numRetries != -1) {
      flinkStreamEnv.setNumberOfExecutionRetries(numRetries);
    }
    long retryDelay = options.getExecutionRetryDelay();
    if (retryDelay != -1) {
      flinkStreamEnv.getConfig().setExecutionRetryDelay(retryDelay);
    }

    // A value of -1 corresponds to disabled checkpointing (see CheckpointConfig in Flink).
    // If the value is not -1, then the validity checks are applied.
    // By default, checkpointing is disabled.
    long checkpointInterval = options.getCheckpointingInterval();
    if (checkpointInterval != -1) {
      if (checkpointInterval < 1) {
        throw new IllegalArgumentException("The checkpoint interval must be positive");
      }
      flinkStreamEnv.enableCheckpointing(checkpointInterval, options.getCheckpointingMode());
      if (options.getCheckpointTimeoutMillis() != -1) {
        flinkStreamEnv
            .getCheckpointConfig()
            .setCheckpointTimeout(options.getCheckpointTimeoutMillis());
      }
      boolean externalizedCheckpoint = options.isExternalizedCheckpointsEnabled();
      boolean retainOnCancellation = options.getRetainExternalizedCheckpointsOnCancellation();
      if (externalizedCheckpoint) {
        flinkStreamEnv
            .getCheckpointConfig()
            .enableExternalizedCheckpoints(
                retainOnCancellation
                    ? ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
                    : ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
      }

      long minPauseBetweenCheckpoints = options.getMinPauseBetweenCheckpoints();
      if (minPauseBetweenCheckpoints != -1) {
        flinkStreamEnv
            .getCheckpointConfig()
            .setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
      }
    } else {
      // https://issues.apache.org/jira/browse/FLINK-2491
      // Checkpointing is disabled, we can allow shutting down sources when they're done
      options.setShutdownSourcesOnFinalWatermark(true);
    }

    applyLatencyTrackingInterval(flinkStreamEnv.getConfig(), options);

    if (options.getAutoWatermarkInterval() != null) {
      flinkStreamEnv.getConfig().setAutoWatermarkInterval(options.getAutoWatermarkInterval());
    }

    // State backend
    final StateBackend stateBackend = options.getStateBackend();
    if (stateBackend != null) {
      flinkStreamEnv.setStateBackend(stateBackend);
    }

    return flinkStreamEnv;
  }

  private static int determineParallelism(
      final int pipelineOptionsParallelism,
      final int envParallelism,
      final Configuration configuration) {
    if (pipelineOptionsParallelism > 0) {
      return pipelineOptionsParallelism;
    }
    if (envParallelism > 0) {
      // If the user supplies a parallelism on the command-line, this is set on the execution environment during creation
      return envParallelism;
    }

    final int flinkConfigParallelism =
        configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM.key(), -1);
    if (flinkConfigParallelism > 0) {
      return flinkConfigParallelism;
    }
    LOG.warn(
        "No default parallelism could be found. Defaulting to parallelism 1. "
            + "Please set an explicit parallelism with --parallelism");
    return 1;
  }

  private static Configuration getFlinkConfiguration(@Nullable String flinkConfDir) {
    return flinkConfDir == null
        ? GlobalConfiguration.loadConfiguration()
        : GlobalConfiguration.loadConfiguration(flinkConfDir);
  }

  private static void applyLatencyTrackingInterval(
      ExecutionConfig config, FlinkPipelineOptions options) {
    long latencyTrackingInterval = options.getLatencyTrackingInterval();
    config.setLatencyTrackingInterval(latencyTrackingInterval);
  }

  /**
   * Remote stream environment that supports job execution with restore from savepoint.
   *
   * <p>This class can be removed once Flink provides this functionality.
   *
   * <p>TODO: https://issues.apache.org/jira/browse/BEAM-5396
   */
  private static class BeamFlinkRemoteStreamEnvironment extends RemoteStreamEnvironment {
    private final SavepointRestoreSettings restoreSettings;

    public BeamFlinkRemoteStreamEnvironment(
        String host,
        int port,
        Configuration clientConfiguration,
        SavepointRestoreSettings restoreSettings,
        String... jarFiles) {
      super(host, port, clientConfiguration, jarFiles, null);
      this.restoreSettings = restoreSettings;
    }

    // copied from RemoteStreamEnvironment and augmented to pass savepoint restore settings
    @Override
    protected JobExecutionResult executeRemotely(StreamGraph streamGraph, List<URL> jarFiles)
        throws ProgramInvocationException {

      List<URL> globalClasspaths = Collections.emptyList();
      String host = super.getHost();
      int port = super.getPort();

      if (LOG.isInfoEnabled()) {
        LOG.info("Running remotely at {}:{}", host, port);
      }

      ClassLoader usercodeClassLoader =
          JobWithJars.buildUserCodeClassLoader(
              jarFiles, globalClasspaths, getClass().getClassLoader());

      Configuration configuration = new Configuration();
      configuration.addAll(super.getClientConfiguration());

      configuration.setString(JobManagerOptions.ADDRESS, host);
      configuration.setInteger(JobManagerOptions.PORT, port);

      configuration.setInteger(RestOptions.PORT, port);

      final ClusterClient<?> client;
      try {
        if (CoreOptions.LEGACY_MODE.equals(configuration.getString(CoreOptions.MODE))) {
          client = new StandaloneClusterClient(configuration);
        } else {
          client = new RestClusterClient<>(configuration, "RemoteStreamEnvironment");
        }
      } catch (Exception e) {
        throw new ProgramInvocationException(
            "Cannot establish connection to JobManager: " + e.getMessage(), e);
      }

      client.setPrintStatusDuringExecution(getConfig().isSysoutLoggingEnabled());

      try {
        return client
            .run(streamGraph, jarFiles, globalClasspaths, usercodeClassLoader, restoreSettings)
            .getJobExecutionResult();
      } catch (ProgramInvocationException e) {
        throw e;
      } catch (Exception e) {
        String term = e.getMessage() == null ? "." : (": " + e.getMessage());
        throw new ProgramInvocationException("The program execution failed" + term, e);
      } finally {
        try {
          client.shutdown();
        } catch (Exception e) {
          LOG.warn("Could not properly shut down the cluster client.", e);
        }
      }
    }
  }
}
