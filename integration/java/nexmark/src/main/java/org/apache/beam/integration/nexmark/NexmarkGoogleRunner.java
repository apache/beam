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
package org.apache.beam.integration.nexmark;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.PipelineResult;
import org.joda.time.Duration;

/**
 * Run a singe Nexmark query using a given configuration on Google Dataflow.
 */
class NexmarkGoogleRunner extends NexmarkRunner<NexmarkGoogleDriver.NexmarkGoogleOptions> {

  public NexmarkGoogleRunner(NexmarkGoogleDriver.NexmarkGoogleOptions options) {
    super(options);
  }

  @Override
  protected boolean isStreaming() {
    return options.isStreaming();
  }

  @Override
  protected int coresPerWorker() {
    String machineType = options.getWorkerMachineType();
    if (machineType == null || machineType.isEmpty()) {
      return 1;
    }
    String[] split = machineType.split("-");
    if (split.length != 3) {
      return 1;
    }
    try {
      return Integer.parseInt(split[2]);
    } catch (NumberFormatException ex) {
      return 1;
    }
  }

  @Override
  protected int maxNumWorkers() {
    return Math.max(options.getNumWorkers(), options.getMaxNumWorkers());
  }

  @Override
  protected boolean canMonitor() {
    return true;
  }

  @Override
  protected String getJobId(PipelineResult job) {
    return ((DataflowPipelineJob) job).getJobId();
  }

  @Override
  protected void invokeBuilderForPublishOnlyPipeline(PipelineBuilder builder) {
    String jobName = options.getJobName();
    String appName = options.getAppName();
    options.setJobName("p-" + jobName);
    options.setAppName("p-" + appName);
    int coresPerWorker = coresPerWorker();
    int eventGeneratorWorkers = (configuration.numEventGenerators + coresPerWorker - 1)
                                / coresPerWorker;
    options.setMaxNumWorkers(Math.min(options.getMaxNumWorkers(), eventGeneratorWorkers));
    options.setNumWorkers(Math.min(options.getNumWorkers(), eventGeneratorWorkers));
    publisherMonitor = new Monitor<Event>(queryName, "publisher");
    try {
      builder.build(options);
    } finally {
      options.setJobName(jobName);
      options.setAppName(appName);
      options.setMaxNumWorkers(options.getMaxNumWorkers());
      options.setNumWorkers(options.getNumWorkers());
    }
  }

  /**
   * Monitor the progress of the publisher job. Return when it has been generating events for
   * at least {@code configuration.preloadSeconds}.
   */
  @Override
  protected void waitForPublisherPreload() {
    checkNotNull(publisherMonitor);
    checkNotNull(publisherResult);
    if (!options.getMonitorJobs()) {
      return;
    }
    if (!(publisherResult instanceof DataflowPipelineJob)) {
      return;
    }
    if (configuration.preloadSeconds <= 0) {
      return;
    }

    NexmarkUtils.console("waiting for publisher to pre-load");

    DataflowPipelineJob job = (DataflowPipelineJob) publisherResult;

    long numEvents = 0;
    long startMsSinceEpoch = -1;
    long endMsSinceEpoch = -1;
    while (true) {
      PipelineResult.State state = job.getState();
      switch (state) {
        case UNKNOWN:
          // Keep waiting.
          NexmarkUtils.console("%s publisher (%d events)", state, numEvents);
          break;
        case STOPPED:
        case DONE:
        case CANCELLED:
        case FAILED:
        case UPDATED:
          NexmarkUtils.console("%s publisher (%d events)", state, numEvents);
          return;
        case RUNNING:
          numEvents = getLong(job, publisherMonitor.getElementCounter());
          if (startMsSinceEpoch < 0 && numEvents > 0) {
            startMsSinceEpoch = System.currentTimeMillis();
            endMsSinceEpoch = startMsSinceEpoch
                              + Duration.standardSeconds(configuration.preloadSeconds).getMillis();
          }
          if (endMsSinceEpoch < 0) {
            NexmarkUtils.console("%s publisher (%d events)", state, numEvents);
          } else {
            long remainMs = endMsSinceEpoch - System.currentTimeMillis();
            if (remainMs > 0) {
              NexmarkUtils.console("%s publisher (%d events, waiting for %ds)", state, numEvents,
                  remainMs / 1000);
            } else {
              NexmarkUtils.console("publisher preloaded %d events", numEvents);
              return;
            }
          }
          break;
      }

      try {
        Thread.sleep(PERF_DELAY.getMillis());
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException("Interrupted: publisher still running.");
      }
    }
  }

}
