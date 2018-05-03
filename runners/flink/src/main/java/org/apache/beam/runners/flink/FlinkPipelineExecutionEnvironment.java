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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The class that instantiates and manages the execution of a given job.
 * Depending on if the job is a Streaming or Batch processing one, it creates
 * the adequate execution environment ({@link ExecutionEnvironment}
 * or {@link StreamExecutionEnvironment}), the necessary {@link FlinkPipelineTranslator}
 * ({@link FlinkBatchPipelineTranslator} or {@link FlinkStreamingPipelineTranslator}) to
 * transform the Beam job into a Flink one, and executes the (translated) job.
 */
class FlinkPipelineExecutionEnvironment {

  private final FlinkPipelineOptions options;

  /**
   * The Flink Batch execution environment. This is instantiated to either a
   * {@link org.apache.flink.api.java.CollectionEnvironment},
   * a {@link org.apache.flink.api.java.LocalEnvironment} or
   * a {@link org.apache.flink.api.java.RemoteEnvironment}, depending on the configuration
   * options.
   */
  private ExecutionEnvironment flinkBatchEnv;

  /**
   * The Flink Streaming execution environment. This is instantiated to either a
   * {@link org.apache.flink.streaming.api.environment.LocalStreamEnvironment} or
   * a {@link org.apache.flink.streaming.api.environment.RemoteStreamEnvironment}, depending
   * on the configuration options, and more specifically, the url of the master.
   */
  private StreamExecutionEnvironment flinkStreamEnv;

  /**
   * Creates a {@link FlinkPipelineExecutionEnvironment} with the user-specified parameters in the
   * provided {@link FlinkPipelineOptions}.
   *
   * @param options the user-defined pipeline options.
   * */
  FlinkPipelineExecutionEnvironment(FlinkPipelineOptions options) {
    this.options = checkNotNull(options);
  }

  /**
   * Depending on if the job is a Streaming or a Batch one, this method creates
   * the necessary execution environment and pipeline translator, and translates
   * the {@link org.apache.beam.sdk.values.PCollection} program into
   * a {@link org.apache.flink.api.java.DataSet}
   * or {@link org.apache.flink.streaming.api.datastream.DataStream} one.
   * */
  public void translate(FlinkRunner flinkRunner, Pipeline pipeline) {
    this.flinkBatchEnv = null;
    this.flinkStreamEnv = null;

    // Serialize and rehydrate pipeline to make sure we only depend serialized transforms.
    try {
      pipeline = PipelineTranslation.fromProto(PipelineTranslation.toProto(pipeline));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    PipelineTranslationOptimizer optimizer =
        new PipelineTranslationOptimizer(TranslationMode.BATCH, options);

    optimizer.translate(pipeline);
    TranslationMode translationMode = optimizer.getTranslationMode();

    pipeline.replaceAll(FlinkTransformOverrides.getDefaultOverrides(
        translationMode == TranslationMode.STREAMING));

    FlinkPipelineTranslator translator;
    if (translationMode == TranslationMode.STREAMING) {
      this.flinkStreamEnv = FlinkExecutionEnvironments.createStreamExecutionEnvironment(options);
      translator = new FlinkStreamingPipelineTranslator(flinkRunner, flinkStreamEnv, options);
    } else {
      this.flinkBatchEnv = FlinkExecutionEnvironments.createBatchExecutionEnvironment(options);
      translator = new FlinkBatchPipelineTranslator(flinkBatchEnv, options);
    }

    translator.translate(pipeline);
  }

  /**
   * Launches the program execution.
   * */
  public JobExecutionResult executePipeline() throws Exception {
    final String jobName = options.getJobName();

    if (flinkBatchEnv != null) {
      return flinkBatchEnv.execute(jobName);
    } else if (flinkStreamEnv != null) {
      return flinkStreamEnv.execute(jobName);
    } else {
      throw new IllegalStateException("The Pipeline has not yet been translated.");
    }
  }

}
