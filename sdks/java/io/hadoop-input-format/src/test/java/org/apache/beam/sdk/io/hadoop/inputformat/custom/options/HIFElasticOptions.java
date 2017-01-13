package org.apache.beam.sdk.io.hadoop.inputformat.custom.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

public interface HIFElasticOptions extends TestPipelineOptions {

  @Description("Elastic server IP")
  @Default.String("elasticServerIp")
  String getElasticServerIp();

  void setElasticServerIp(String elasticServerIp);

  @Description("Elastic server port")
  @Default.Integer(0)
  Integer getElasticServerPort();

  void setElasticServerPort(Integer elasticServerPort);

}
