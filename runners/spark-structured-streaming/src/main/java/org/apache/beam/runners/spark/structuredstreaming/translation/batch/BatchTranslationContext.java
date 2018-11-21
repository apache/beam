package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Keeps track of the {@link Dataset} and the step the translation is in.
 */
public class BatchTranslationContext {
  private final Map<PValue, Dataset<?>> datasets;

  /**
   * For keeping track about which DataSets don't have a successor. We need to terminate these with
   * a discarding sink because the Beam model allows dangling operations.
   */
  private final Map<PValue, Dataset<?>> danglingDataSets;

  private final SparkSession sparkSession;
  private final SparkPipelineOptions options;

  private AppliedPTransform<?, ?, ?> currentTransform;


  public BatchTranslationContext(SparkSession sparkSession, SparkPipelineOptions options) {
    this.sparkSession = sparkSession;
    this.options = options;
    this.datasets = new HashMap<>();
    this.danglingDataSets = new HashMap<>();
  }

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }
}
