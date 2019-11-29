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

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.runners.core.metrics.MetricsPusher;
import org.apache.beam.runners.spark.structuredstreaming.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.metrics.AggregatorMetricSource;
import org.apache.beam.runners.spark.structuredstreaming.metrics.CompositeSource;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.metrics.SparkBeamMetricSource;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.PipelineTranslatorBatch;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.PipelineTranslatorStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.spark.SparkEnv$;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.metrics.MetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SparkStructuredStreamingRunner is based on spark structured streaming framework and is no more
 * based on RDD/DStream API. See
 * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html It is still
 * experimental, its coverage of the Beam model is partial. The SparkStructuredStreamingRunner
 * translate operations defined on a pipeline to a representation executable by Spark, and then
 * submitting the job to Spark to be executed. If we wanted to run a Beam pipeline with the default
 * options of a single threaded spark instance in local mode, we would do the following:
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
@Experimental(value = Experimental.Kind.WITH_EXCEPTIONS)
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
          detectClassPathResourcesToStage(
              SparkStructuredStreamingRunner.class.getClassLoader(), options));
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
    MetricsEnvironment.setMetricsSupported(true);

    LOG.info(
        "*** SparkStructuredStreamingRunner is based on spark structured streaming framework and is no more \n"
            + " based on RDD/DStream API. See\n"
            + " https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html\n"
            + " It is still experimental, its coverage of the Beam model is partial. ***");

    // clear state of Aggregators, Metrics and Watermarks if exists.
    AggregatorsAccumulator.clear();
    MetricsAccumulator.clear();

    final TranslationContext translationContext = translatePipeline(pipeline);

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final Future<?> submissionFuture =
        executorService.submit(
            () -> {
              // TODO initialise other services: checkpointing, metrics system, listeners, ...
              translationContext.startPipeline();
            });
    executorService.shutdown();

    // TODO: Streaming.
    SparkStructuredStreamingPipelineResult result =
        new SparkStructuredStreamingPipelineResult(
            submissionFuture, translationContext.getSparkSession());

    if (options.getEnableSparkMetricSinks()) {
      registerMetricsSource(options.getAppName());
    }

    MetricsPusher metricsPusher =
        new MetricsPusher(
            MetricsAccumulator.getInstance().value(), options.as(MetricsOptions.class), result);
    metricsPusher.start();

    if (options.getTestMode()) {
      result.waitUntilFinish();
      result.stop();
    }

    return result;
  }

  private TranslationContext translatePipeline(Pipeline pipeline) {
    PipelineTranslator.detectTranslationMode(pipeline, options);
    PipelineTranslator.replaceTransforms(pipeline, options);
    PipelineTranslator.prepareFilesToStageForRemoteClusterExecution(options);
    PipelineTranslator pipelineTranslator =
        options.isStreaming()
            ? new PipelineTranslatorStreaming(options)
            : new PipelineTranslatorBatch(options);

    final JavaSparkContext jsc =
        JavaSparkContext.fromSparkContext(
            pipelineTranslator.getTranslationContext().getSparkSession().sparkContext());
    initAccumulators(options, jsc);

    pipelineTranslator.translate(pipeline);
    return pipelineTranslator.getTranslationContext();
  }

  private void registerMetricsSource(String appName) {
    final MetricsSystem metricsSystem = SparkEnv$.MODULE$.get().metricsSystem();
    final AggregatorMetricSource aggregatorMetricSource =
        new AggregatorMetricSource(null, AggregatorsAccumulator.getInstance().value());
    final SparkBeamMetricSource metricsSource = new SparkBeamMetricSource(null);
    final CompositeSource compositeSource =
        new CompositeSource(
            appName + ".Beam",
            metricsSource.metricRegistry(),
            aggregatorMetricSource.metricRegistry());
    // re-register the metrics in case of context re-use
    metricsSystem.removeSource(compositeSource);
    metricsSystem.registerSource(compositeSource);
  }

  /** Init Metrics/Aggregators accumulators. This method is idempotent. */
  public static void initAccumulators(
      SparkStructuredStreamingPipelineOptions opts, JavaSparkContext jsc) {
    // Init metrics accumulators
    MetricsAccumulator.init(jsc);
    AggregatorsAccumulator.init(jsc);
  }
}
