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
package org.apache.beam.runners.spark.structuredstreaming;

import static org.apache.beam.runners.core.construction.PipelineResources.detectClassPathResourcesToStage;

import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.PipelineTranslatorBatch;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.PipelineTranslatorStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SparkStructuredStreamingRunner translate operations defined on a pipeline to a representation
 * executable by Spark, and then submitting the job to Spark to be executed. If we wanted to run a
 * Beam pipeline with the default options of a single threaded spark instance in local mode, we
 * would do the following:
 *
 * <p>{@code Pipeline p = [logic for pipeline creation] SparkStructuredStreamingPipelineResult
 * result = (SparkStructuredStreamingPipelineResult) p.run(); }
 *
 * <p>To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * <p>{@code Pipeline p = [logic for pipeline creation] SparkStructuredStreamingPipelineOptions
 * options = SparkPipelineOptionsFactory.create(); options.setSparkMaster("spark://host:port");
 * SparkStructuredStreamingPipelineResult result = (SparkStructuredStreamingPipelineResult) p.run();
 * }
 */
public final class SparkStructuredStreamingRunner
    extends PipelineRunner<SparkStructuredStreamingPipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkStructuredStreamingRunner.class);

  /** Options used in this pipeline runner. */
  private final SparkStructuredStreamingPipelineOptions options;

  /**
   * Creates and returns a new SparkStructuredStreamingRunner with default options. In particular,
   * against a spark instance running in local mode.
   *
   * @return A pipeline runner with default options.
   */
  public static SparkStructuredStreamingRunner create() {
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.as(SparkStructuredStreamingPipelineOptions.class);
    options.setRunner(SparkStructuredStreamingRunner.class);
    return new SparkStructuredStreamingRunner(options);
  }

  /**
   * Creates and returns a new SparkStructuredStreamingRunner with specified options.
   *
   * @param options The SparkStructuredStreamingPipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkStructuredStreamingRunner create(
      SparkStructuredStreamingPipelineOptions options) {
    return new SparkStructuredStreamingRunner(options);
  }

  /**
   * Creates and returns a new SparkStructuredStreamingRunner with specified options.
   *
   * @param options The PipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkStructuredStreamingRunner fromOptions(PipelineOptions options) {
    SparkStructuredStreamingPipelineOptions sparkOptions =
        PipelineOptionsValidator.validate(SparkStructuredStreamingPipelineOptions.class, options);

    if (sparkOptions.getFilesToStage() == null) {
      sparkOptions.setFilesToStage(
          detectClassPathResourcesToStage(SparkStructuredStreamingRunner.class.getClassLoader()));
      LOG.info(
          "PipelineOptions.filesToStage was not specified. "
              + "Defaulting to files from the classpath: will stage {} files. "
              + "Enable logging at DEBUG level to see which files will be staged.",
          sparkOptions.getFilesToStage().size());
      LOG.debug("Classpath elements: {}", sparkOptions.getFilesToStage());
    }

    return new SparkStructuredStreamingRunner(sparkOptions);
  }

  /**
   * No parameter constructor defaults to running this pipeline in Spark's local mode, in a single
   * thread.
   */
  private SparkStructuredStreamingRunner(SparkStructuredStreamingPipelineOptions options) {
    this.options = options;
  }

  @Override
  public SparkStructuredStreamingPipelineResult run(final Pipeline pipeline) {
    TranslationContext translationContext = translatePipeline(pipeline);
    // TODO initialise other services: checkpointing, metrics system, listeners, ...
    // TODO pass testMode using pipelineOptions
    translationContext.startPipeline(true);
    return new SparkStructuredStreamingPipelineResult();
  }

  private TranslationContext translatePipeline(Pipeline pipeline) {
    PipelineTranslator.detectTranslationMode(pipeline, options);
    PipelineTranslator.replaceTransforms(pipeline, options);
    PipelineTranslator.prepareFilesToStageForRemoteClusterExecution(options);
    PipelineTranslator pipelineTranslator =
        options.isStreaming()
            ? new PipelineTranslatorStreaming(options)
            : new PipelineTranslatorBatch(options);
    pipelineTranslator.translate(pipeline);
    return pipelineTranslator.getTranslationContext();
  }
}
