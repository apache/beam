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
package org.apache.beam.sdk.options;

import java.util.List;
import org.apache.beam.sdk.options.Validation.Required;

/** Pipeline options common to all portable runners. */
public interface PortablePipelineOptions extends PipelineOptions {

  // TODO: https://issues.apache.org/jira/browse/BEAM-4106: Consider pulling this out into a new
  // options interface, e.g., FileStagingOptions.
  /**
   * List of local files to make available to workers.
   *
   * <p>Files are placed on the worker's classpath.
   *
   * <p>The default value is the list of jars from the main program's classpath.
   */
  @Description(
      "Files to stage to the artifact service and make available to workers. Files are placed on "
          + "the worker's classpath. The default value is all files from the classpath.")
  List<String> getFilesToStage();
  void setFilesToStage(List<String> value);

  @Description(
      "Job service endpoint to use. Should be in the form of address and port, e.g. localhost:3000")
  @Required
  String getJobEndpoint();
  void setJobEndpoint(String endpoint);
}
