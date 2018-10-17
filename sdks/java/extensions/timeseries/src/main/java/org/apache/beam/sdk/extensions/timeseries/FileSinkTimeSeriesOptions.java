/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.timeseries;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/** Pipeline options that allow configuring the output path. */
@Experimental
public interface FileSinkTimeSeriesOptions extends TimeSeriesOptions {

  @Description(
      "File output location for pipelines that make use of directory based sink transforms. for example /tmp or gs:/bucket/tmp ")
  @Validation.Required
  String getFileSinkDirectory();

  void setFileSinkDirectory(String directory);
}
