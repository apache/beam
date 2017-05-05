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

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobRetrievalException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

/**
 * A {@link FlinkStreamingPipelineJob} that runs on a local {@link LocalFlinkMiniCluster}.
 */
class FlinkLocalStreamingPipelineJob extends FlinkStreamingPipelineJob {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlinkLocalStreamingPipelineJob.class);


  private final LocalFlinkMiniCluster flinkMiniCluster;
  private final JobID jobId;
  private final FiniteDuration clientTimeout = FiniteDuration.apply(10, "seconds");

  /**
   * For protecting access to the final accumulators and the final state. We need to shutdown the
   * mini cluster and update the final state/accumulators under lock protection.
   */
  private final Object clusterShutdownLock = new Object();

  /**
   * We set these when the job finishes and we retrieve the final accumulators.
   */
  private volatile Map<String, Object> finalAccumulators = null;

  /**
   * We keep track of this so that the query methods can cancel early because when the job is done
   * the LocalFlinkMiniCluster does not allow any more querying.
   */
  private volatile State finalState = null;

  public FlinkLocalStreamingPipelineJob(
      FlinkPipelineOptions pipelineOptions,
      LocalStreamEnvironment flinkEnv) throws Exception {

    // transform the streaming program into a JobGraph
    StreamGraph streamGraph = flinkEnv.getStreamGraph();
    streamGraph.setJobName(pipelineOptions.getJobName());

    JobGraph jobGraph = streamGraph.getJobGraph();

    Configuration configuration = new Configuration();
    configuration.addAll(jobGraph.getJobConfiguration());

    configuration.setInteger(
        ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

    if (LOG.isInfoEnabled()) {
      LOG.info("Running job on local embedded Flink mini cluster");
    }

    flinkMiniCluster = new LocalFlinkMiniCluster(configuration, false);
    flinkMiniCluster.start();

    // submit detached and get the JobId, so that we don't block
    jobId = flinkMiniCluster.submitJobDetached(jobGraph).getJobID();

    LOG.info("Submitted job with JobId {}", jobId);

    // start a Thread that waits on the job and shuts down the mini cluster when
    // the job is done
    Thread shutdownTread = new Thread() {
      @Override
      public void run() {
        try {
          StandaloneClusterClient clusterClient;
          try {
            clusterClient = new StandaloneClusterClient(flinkMiniCluster.configuration());
          } catch (Exception e) {
            LOG.error("Error creating Cluster Client.", e);
            return;
          }

          try {
            // this call will only return when the job finishes
            JobExecutionResult jobExecutionResult = clusterClient.retrieveJob(jobId);

            synchronized (clusterShutdownLock) {
              finalAccumulators = jobExecutionResult.getAllAccumulatorResults();
              finalState = PipelineResult.State.DONE;
              flinkMiniCluster.stop();
            }
          } catch (JobRetrievalException e) {
            // job could already be finished, try and get the accumulator results
            synchronized (clusterShutdownLock) {
              try {
                finalAccumulators = clusterClient.getAccumulators(
                    getJobId(), Thread.currentThread().getContextClassLoader());
                finalState = FlinkLocalStreamingPipelineJob.this.getState();
              } catch (Exception e1) {
                LOG.error(
                    "Error while getting accumulator results for (possibly) finished job.", e1);
                // set to an empty map so the the code in #metrics() doesn't try to fetch
                // accumulators
                finalAccumulators = new HashMap<>();
                finalState = PipelineResult.State.FAILED;
              }
              flinkMiniCluster.stop();
            }
          } catch (JobExecutionException e) {
            LOG.error("Exception caught while waiting on job.", e);
            synchronized (clusterShutdownLock) {
              finalAccumulators = new HashMap<>();
              if (e instanceof JobCancellationException) {
                finalState = PipelineResult.State.CANCELLED;
              } else {
                finalState = PipelineResult.State.FAILED;
              }
              flinkMiniCluster.stop();
            }
          }
        } finally {
          flinkMiniCluster.shutdown();
        }
      }
    };
    shutdownTread.setName("Beam Flink Runner Local Cluster Shutdown Thread");
    shutdownTread.start();
  }

  @Override
  public State cancel() throws IOException {
    synchronized (clusterShutdownLock) {
      if (finalState != null) {
        return finalState;
      }
      return super.cancel();
    }
  }

  @Override
  public State getState() {
    synchronized (clusterShutdownLock) {
      if (finalState != null) {
        return finalState;
      }
      return super.getState();
    }
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    if (finalState != null) {
      return finalState;
    }
    return super.waitUntilFinish(duration);
  }

  @Override
  public State waitUntilFinish() {
    if (finalState != null) {
      return finalState;
    }
    return super.waitUntilFinish();
  }

  @Override
  public MetricResults metrics() {
    // return a wrapper, so that every time queryMetrics() is called we query
    // the Flink Accumulators
    return new MetricResults() {
      @Override
      public MetricQueryResults queryMetrics(MetricsFilter filter) {
        synchronized (clusterShutdownLock) {
          if (finalAccumulators != null) {
            // return the final accumulators we got before the cluster shut down
            return asAttemptedOnlyMetricResults(
                (MetricsContainerStepMap) finalAccumulators.
                    get(FlinkMetricContainer.ACCUMULATOR_NAME))
                .queryMetrics(filter);
          } else {
            StandaloneClusterClient clusterClient;
            try {
              clusterClient = new StandaloneClusterClient(getConfiguration());
            } catch (Exception e) {
              throw new RuntimeException("Error retrieving cluster client.", e);
            }

            try {
              Map<String, Object> accumulators = clusterClient.getAccumulators(
                  getJobId(), Thread.currentThread().getContextClassLoader());

              // at the beginning it can happen that accumulators are not yet available
              if (accumulators.isEmpty()) {
                return asAttemptedOnlyMetricResults(new MetricsContainerStepMap())
                    .queryMetrics(filter);
              }

              return asAttemptedOnlyMetricResults(
                  (MetricsContainerStepMap) accumulators.get(FlinkMetricContainer.ACCUMULATOR_NAME))
                  .queryMetrics(filter);
            } catch (Exception e) {
              throw new RuntimeException("Could not retrieve Accumulators from JobManager.", e);
            }
          }
        }
      }
    };
  }


  @Override
  protected Configuration getConfiguration() {
    return flinkMiniCluster.configuration();
  }

  @Override
  protected FiniteDuration getClientTimeout() {
    return clientTimeout;
  }

  @Override
  public JobID getJobId() {
    return jobId;
  }
}
