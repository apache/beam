package org.apache.beam.sdk.io.hadoop.inputformat.custom.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * Properties needed when using HadoopInputFormatIO with the Beam SDK.
 *
 */
public interface HIFTestOptions extends TestPipelineOptions {

  @Description("Server IP")
  @Default.String("serverIp")
  String getServerIp();

  void setServerIp(String serverIp);

  @Description("Server port")
  @Default.Integer(0)
  Integer getServerPort();

  void setServerPort(Integer serverPort);

}
