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
package org.apache.beam.examples.subprocess.utils;

import org.apache.beam.examples.subprocess.configuration.SubProcessConfiguration;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Contains the configuration for the external library. */
@DefaultCoder(AvroCoder.class)
public class ExecutableFile {

  String fileName;

  private String sourceGCSLocation;
  private String destinationLocation;

  static final Logger LOG = LoggerFactory.getLogger(ExecutableFile.class);

  public String getSourceGCSLocation() {
    return sourceGCSLocation;
  }

  public void setSourceGCSLocation(String sourceGCSLocation) {
    this.sourceGCSLocation = sourceGCSLocation;
  }

  public String getDestinationLocation() {
    return destinationLocation;
  }

  public void setDestinationLocation(String destinationLocation) {
    this.destinationLocation = destinationLocation;
  }

  public ExecutableFile(SubProcessConfiguration configuration, String fileName)
      throws IllegalStateException {
    if (configuration == null) {
      throw new IllegalStateException("Configuration can not be NULL");
    }
    if (fileName == null) {
      throw new IllegalStateException("FileName can not be NULLt");
    }
    this.fileName = fileName;
    setDestinationLocation(configuration);
    setSourceLocation(configuration);
  }

  private void setDestinationLocation(SubProcessConfiguration configuration) {
    this.sourceGCSLocation =
        FileUtils.getFileResourceId(configuration.getSourcePath(), fileName).toString();
  }

  private void setSourceLocation(SubProcessConfiguration configuration) {
    this.destinationLocation =
        FileUtils.getFileResourceId(configuration.getWorkerPath(), fileName).toString();
  }
}
