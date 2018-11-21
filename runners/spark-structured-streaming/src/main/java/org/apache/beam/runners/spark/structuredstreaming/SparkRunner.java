package org.apache.beam.runners.spark.structuredstreaming;

import static org.apache.beam.runners.core.construction.PipelineResources.detectClassPathResourcesToStage;

import org.apache.beam.runners.spark.structuredstreaming.translation.batch.BatchPipelineTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.PipelineTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.StreamingPipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SparkRunner translate operations defined on a pipeline to a representation executable by
 * Spark, and then submitting the job to Spark to be executed. If we wanted to run a Beam pipeline
 * with the default options of a single threaded spark instance in local mode, we would do the
 * following:
 *
 * <p>{@code Pipeline p = [logic for pipeline creation] SparkPipelineResult result =
 * (SparkPipelineResult) p.run(); }
 *
 * <p>To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * <p>{@code Pipeline p = [logic for pipeline creation] SparkPipelineOptions options =
 * SparkPipelineOptionsFactory.create(); options.setSparkMaster("spark://host:port");
 * SparkPipelineResult result = (SparkPipelineResult) p.run(); }
 */
public final class SparkRunner extends PipelineRunner<SparkPipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRunner.class);

  /** Options used in this pipeline runner. */
  private final SparkPipelineOptions options;

  /**
   * Creates and returns a new SparkRunner with default options. In particular, against a spark
   * instance running in local mode.
   *
   * @return A pipeline runner with default options.
   */
  public static SparkRunner create() {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    return new SparkRunner(options);
  }

  /**
   * Creates and returns a new SparkRunner with specified options.
   *
   * @param options The SparkPipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkRunner create(SparkPipelineOptions options) {
    return new SparkRunner(options);
  }

  /**
   * Creates and returns a new SparkRunner with specified options.
   *
   * @param options The PipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkRunner fromOptions(PipelineOptions options) {
    SparkPipelineOptions sparkOptions = PipelineOptionsValidator
        .validate(SparkPipelineOptions.class, options);

    if (sparkOptions.getFilesToStage() == null) {
      sparkOptions.setFilesToStage(detectClassPathResourcesToStage(SparkRunner.class.getClassLoader()));
      LOG.info("PipelineOptions.filesToStage was not specified. "
              + "Defaulting to files from the classpath: will stage {} files. "
              + "Enable logging at DEBUG level to see which files will be staged.",
          sparkOptions.getFilesToStage().size());
      LOG.debug("Classpath elements: {}", sparkOptions.getFilesToStage());
    }

    return new SparkRunner(sparkOptions);
  }

  /**
   * No parameter constructor defaults to running this pipeline in Spark's local mode, in a single
   * thread.
   */
  private SparkRunner(SparkPipelineOptions options) {
    this.options = options;
  }

  @Override public SparkPipelineResult run(final Pipeline pipeline) {
    translatePipeline(pipeline);
    executePipeline(pipeline);
    return new SparkPipelineResult();
  }

  private void translatePipeline(Pipeline pipeline){
    PipelineTranslator.detectTranslationMode(pipeline, options);
    PipelineTranslator.replaceTransforms(pipeline, options);
    PipelineTranslator.prepareFilesToStageForRemoteClusterExecution(options);
    PipelineTranslator pipelineTranslator = options.isStreaming() ? new StreamingPipelineTranslator(options) : new BatchPipelineTranslator(options);
    pipelineTranslator.translate(pipeline);
  }
  private void executePipeline(Pipeline pipeline) {}

}
