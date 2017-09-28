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
package org.apache.beam.sdk.io.tika;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * TikaInput Options to support the command-line applications.
 */
public interface TikaOptions extends PipelineOptions {

  @Description("Input path")
  @Validation.Required
  String getInput();
  void setInput(String value);

  @Description("Tika Config path")
  String getTikaConfigPath();
  void setTikaConfigPath(String value);

  @Description("Tika Parser Content Type hint")
  String getContentTypeHint();
  void setContentTypeHint(String value);

  @Description("Pipeline name")
  @Default.String("TikaRead")
  String getPipelineName();
  void setPipelineName(String value);

  @Description("Output path")
  @Default.String("/tmp/tika/out")
  String getOutput();
  void setOutput(String value);

}
