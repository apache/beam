package org.apache.beam.runners.spark.structuredstreaming.translation;

import org.apache.beam.runners.core.construction.PipelineResources;
import org.apache.beam.runners.spark.SparkTransformOverrides;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Does all the translation work: mode detection, nodes translation.
 */

public class PipelineTranslator extends Pipeline.PipelineVisitor.Defaults{


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
      // set streaming mode if it's a streaming pipeline
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

}
