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
package org.apache.beam.sdk.tpcds;

import static org.apache.beam.sdk.tpcds.SqlTransformRunner.METRICS_NAMESPACE;
import static org.apache.beam.sdk.tpcds.SqlTransformRunner.OUTPUT_COUNTER;

import java.util.concurrent.Callable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** To fulfill multi-threaded execution. */
public class TpcdsRun implements Callable<TpcdsRunResult> {
  private final Pipeline pipeline;

  private static final Logger LOG = LoggerFactory.getLogger(TpcdsRun.class);

  public TpcdsRun(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  @Override
  public TpcdsRunResult call() {
    LOG.info("Run TPC-DS job: {}", pipeline.getOptions().getJobName());
    TpcdsRunResult tpcdsRunResult;

    try {
      long startTimeStamp = System.currentTimeMillis();
      PipelineResult pipelineResult = pipeline.run();
      State state = pipelineResult.waitUntilFinish();
      long endTimeStamp = System.currentTimeMillis();

      // Make sure to set the job status to be successful only when pipelineResult's final state is
      // DONE.
      boolean isSuccessful = state == State.DONE;

      // Check a number of output records - it MUST be greater than 0.
      if (isSuccessful) {
        long outputRecords = 0;
        MetricQueryResults metrics =
            pipelineResult
                .metrics()
                .queryMetrics(
                    MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.named(METRICS_NAMESPACE, OUTPUT_COUNTER))
                        .build());
        if (metrics.getCounters().iterator().hasNext()) {
          // Despite it's iterable, it should contain only one entry
          MetricResult<Long> metricResult = metrics.getCounters().iterator().next();
          if (metricResult.getAttempted() != null && metricResult.getAttempted() > 0) {
            outputRecords = metricResult.getAttempted();
          }
        }

        // It's expected a "greater than zero" number of output records for successful jobs.
        if (outputRecords <= 0) {
          LOG.warn(
              "Output records counter for job \"{}\" is {}",
              pipeline.getOptions().getJobName(),
              outputRecords);
          isSuccessful = false;
        }
      }

      tpcdsRunResult =
          new TpcdsRunResult(
              isSuccessful, startTimeStamp, endTimeStamp, pipeline.getOptions(), pipelineResult);
    } catch (Exception e) {
      // If the pipeline execution failed, return a result with failed status but don't interrupt
      // other threads.
      e.printStackTrace();
      tpcdsRunResult = new TpcdsRunResult(false, 0, 0, pipeline.getOptions(), null);
    }

    return tpcdsRunResult;
  }
}
