package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;


public interface SparkPipelineOptions extends PipelineOptions {
  @Description("The url of the spark master to connect to, (e.g. spark://host:port, local[4]).")
  @Default.String("local[1]")
  String getSparkMaster();
  void setSparkMaster(String master);
}
