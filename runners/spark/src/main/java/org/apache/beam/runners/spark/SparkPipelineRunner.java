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
package org.apache.beam.runners.spark;

import static org.apache.beam.runners.core.construction.PipelineResources.detectClassPathResourcesToStage;
import static org.apache.beam.runners.spark.SparkPipelineOptions.prepareFilesToStageForRemoteClusterExecution;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.PipelineTrimmer;
import org.apache.beam.runners.core.metrics.MetricsPusher;
import org.apache.beam.runners.fnexecution.jobsubmission.PortablePipelineRunner;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.translation.SparkBatchPortablePipelineTranslator;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkTranslationContext;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs a portable pipeline on Apache Spark. */
public class SparkPipelineRunner implements PortablePipelineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPipelineRunner.class);

  private final SparkPipelineOptions pipelineOptions;

  public SparkPipelineRunner(SparkPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public SparkPipelineResult run(RunnerApi.Pipeline pipeline, JobInfo jobInfo) {
    SparkBatchPortablePipelineTranslator translator = new SparkBatchPortablePipelineTranslator();

    // Don't let the fuser fuse any subcomponents of native transforms.
    Pipeline trimmedPipeline = PipelineTrimmer.trim(pipeline, translator.knownUrns());
    Pipeline fusedPipeline = GreedyPipelineFuser.fuse(trimmedPipeline).toPipeline();

    if (pipelineOptions.getFilesToStage() == null) {
      pipelineOptions.setFilesToStage(
          detectClassPathResourcesToStage(SparkPipelineRunner.class.getClassLoader()));
      LOG.info(
          "PipelineOptions.filesToStage was not specified. Defaulting to files from the classpath");
    }
    prepareFilesToStageForRemoteClusterExecution(pipelineOptions);
    LOG.info(
        "Will stage {} files. (Enable logging at DEBUG level to see which files will be staged.)",
        pipelineOptions.getFilesToStage().size());
    LOG.debug("Staging files: {}", pipelineOptions.getFilesToStage());

    final JavaSparkContext jsc = SparkContextFactory.getSparkContext(pipelineOptions);
    LOG.info(String.format("Running job %s on Spark master %s", jobInfo.jobId(), jsc.master()));
    AggregatorsAccumulator.init(pipelineOptions, jsc);

    MetricsEnvironment.setMetricsSupported(false);
    MetricsAccumulator.init(pipelineOptions, jsc);

    final SparkTranslationContext context =
        new SparkTranslationContext(jsc, pipelineOptions, jobInfo);
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final Future<?> submissionFuture =
        executorService.submit(
            () -> {
              translator.translate(fusedPipeline, context);
              LOG.info(
                  String.format(
                      "Job %s: Pipeline translated successfully. Computing outputs",
                      jobInfo.jobId()));
              context.computeOutputs();
              LOG.info(String.format("Job %s finished.", jobInfo.jobId()));
            });

    SparkPipelineResult result = new SparkPipelineResult.BatchMode(submissionFuture, jsc);
    MetricsPusher metricsPusher =
        new MetricsPusher(
            MetricsAccumulator.getInstance().value(),
            pipelineOptions.as(MetricsOptions.class),
            result);
    metricsPusher.start();

    result.waitUntilFinish();
    executorService.shutdown();
    return result;
  }
}
