package org.apache.beam.runners.spark.structuredstreaming.translation;

import org.apache.beam.runners.core.construction.PipelineResources;
import org.apache.beam.runners.spark.SparkTransformOverrides;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.BatchPipelineTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.StreamingPipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 /**
 * The role of this class is to detect the pipeline mode and to translate the Beam operators to their Spark counterparts. If we have
 * a streaming job, this is instantiated as a {@link StreamingPipelineTranslator}. In other
 * case, i.e. for a batch job, a {@link BatchPipelineTranslator} is created. Correspondingly,
 */

public class PipelineTranslator extends Pipeline.PipelineVisitor.Defaults{

  // --------------------------------------------------------------------------------------------
  //  Pipeline preparation methods
  // --------------------------------------------------------------------------------------------
  /**
   * Local configurations work in the same JVM and have no problems with improperly formatted files
   * on classpath (eg. directories with .class files or empty directories). Prepare files for
   * staging only when using remote cluster (passing the master address explicitly).
   */
  public static void prepareFilesToStageForRemoteClusterExecution(SparkPipelineOptions options) {
    if (!options.getSparkMaster().matches("local\\[?\\d*\\]?")) {
      options.setFilesToStage(
          PipelineResources.prepareFilesForStaging(
              options.getFilesToStage(), options.getTempLocation()));
    }
  }

  public static void replaceTransforms(Pipeline pipeline, SparkPipelineOptions options){
    pipeline.replaceAll(SparkTransformOverrides.getDefaultOverrides(options.isStreaming()));

  }


  /** Visit the pipeline to determine the translation mode (batch/streaming) and update options accordingly. */
  public static void detectTranslationMode(Pipeline pipeline, SparkPipelineOptions options) {
    TranslationModeDetector detector = new TranslationModeDetector();
    pipeline.traverseTopologically(detector);
    if (detector.getTranslationMode().equals(TranslationMode.STREAMING)) {
      options.setStreaming(true);
    }
  }

  /** The translation mode of the Beam Pipeline. */
  private enum TranslationMode {

    /** Uses the batch mode. */
    BATCH,

    /** Uses the streaming mode. */
    STREAMING
  }

  /** Traverses the Pipeline to determine the {@link TranslationMode} for this pipeline. */
  private static class TranslationModeDetector extends Pipeline.PipelineVisitor.Defaults {
    private static final Logger LOG = LoggerFactory.getLogger(TranslationModeDetector.class);

    private TranslationMode translationMode;

    TranslationModeDetector(TranslationMode defaultMode) {
      this.translationMode = defaultMode;
    }

    TranslationModeDetector() {
      this(TranslationMode.BATCH);
    }

    TranslationMode getTranslationMode() {
      return translationMode;
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {
      if (translationMode.equals(TranslationMode.BATCH)) {
        if (value instanceof PCollection
            && ((PCollection) value).isBounded() == PCollection.IsBounded.UNBOUNDED) {
          LOG.info(
              "Found unbounded PCollection {}. Switching to streaming execution.", value.getName());
          translationMode = TranslationMode.STREAMING;
        }
      }
    }
  }

  // --------------------------------------------------------------------------------------------
  //  Pipeline utility methods
  // --------------------------------------------------------------------------------------------

  /**
   * Utility formatting method.
   *
   * @param n number of spaces to generate
   * @return String with "|" followed by n spaces
   */
  protected static String genSpaces(int n) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < n; i++) {
      builder.append("|   ");
    }
    return builder.toString();
  }

  // --------------------------------------------------------------------------------------------
  //  Pipeline visitor methods
  // --------------------------------------------------------------------------------------------

  /**
   * Translates the pipeline by passing this class as a visitor.
   *
   * @param pipeline The pipeline to be translated
   */
  public void translate(Pipeline pipeline) {
    pipeline.traverseTopologically(this);
  }





}
