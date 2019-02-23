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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.fnexecution.jobsubmission.PortablePipelineRunner;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkBatchPortablePipelineTranslator;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
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
  public SparkPipelineResult run(final Pipeline pipeline) {
    LOG.info("Executing pipeline using the Spark portable runner.");

    // TODO(ibzib) check if pipeline requires streaming
    SparkBatchPortablePipelineTranslator translator = new SparkBatchPortablePipelineTranslator();

    // TODO(ibzib) fuse the pipeline (BEAM-6762)

    final JavaSparkContext jsc = SparkContextFactory.getSparkContext(pipelineOptions);
    final EvaluationContext evaluationContext = new EvaluationContext(jsc, null, pipelineOptions);
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final Future<?> submissionFuture =
        executorService.submit(
            () -> {
              translator.translate(pipeline, evaluationContext);
              evaluationContext.computeOutputs();
            });

    SparkPipelineResult result = new SparkPipelineResult.BatchMode(submissionFuture, jsc);
    result.waitUntilFinish();
    executorService.shutdown();
    return result;
  }
}
