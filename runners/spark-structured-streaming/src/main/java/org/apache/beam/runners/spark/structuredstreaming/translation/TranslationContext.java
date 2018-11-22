package org.apache.beam.runners.spark.structuredstreaming.translation;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Base class that gives a context for {@link PTransform} translation: keeping track of the datasets,
 * the {@link SparkSession}, the current transform being translated.
 */
public class TranslationContext {

  private AppliedPTransform<?, ?, ?> currentTransform;
  private final Map<PValue, Dataset<?>> datasets;
  private SparkSession sparkSession;
  private final SparkPipelineOptions options;

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

  public TranslationContext(SparkPipelineOptions options) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster(options.getSparkMaster());
    sparkConf.setAppName(options.getAppName());
    if (options.getFilesToStage() != null && !options.getFilesToStage().isEmpty()) {
      sparkConf.setJars(options.getFilesToStage().toArray(new String[0]));
    }

    this.sparkSession = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate();
    this.options = options;
    this.datasets = new HashMap<>();
  }

}
