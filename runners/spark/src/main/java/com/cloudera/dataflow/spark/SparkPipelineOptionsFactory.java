package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

public class SparkPipelineOptionsFactory {
  public static SparkPipelineOptions create() {
    return PipelineOptionsFactory.create(SparkPipelineOptions.class);
  }
}
