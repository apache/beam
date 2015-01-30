/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.common.base.Strings;

/**
 * GCP implementation of {@link PathValidator}. Only GCS paths are allowed.
 */
public class DataflowPathValidator implements PathValidator {

  private DataflowPipelineOptions dataflowOptions;

  private DataflowPathValidator(DataflowPipelineOptions options) {
    this.dataflowOptions = options;
  }

  public static DataflowPathValidator fromOptions(PipelineOptions options) {
    return new DataflowPathValidator(options.as(DataflowPipelineOptions.class));
  }

  @Override
  public void validateAndUpdateOptions() {
    Preconditions.checkArgument(!(Strings.isNullOrEmpty(dataflowOptions.getTempLocation())
        && Strings.isNullOrEmpty(dataflowOptions.getStagingLocation())),
        "Missing required value: at least one of tempLocation or stagingLocation must be set.");
    if (Strings.isNullOrEmpty(dataflowOptions.getTempLocation())) {
      dataflowOptions.setTempLocation(dataflowOptions.getStagingLocation());
    } else if (Strings.isNullOrEmpty(dataflowOptions.getStagingLocation())) {
      dataflowOptions.setStagingLocation(
          GcsPath.fromUri(dataflowOptions.getTempLocation()).resolve("staging").toString());
    }
  }

  @Override
  public String validateInputFilePatternSupported(String filepattern) {
    GcsPath gcsPath = GcsPath.fromUri(filepattern);
    Preconditions.checkArgument(
        dataflowOptions.getGcsUtil().isGcsPatternSupported(gcsPath.getObject()));
    return verifyGcsPath(filepattern);
  }

  @Override
  public String validateOutputFilePrefixSupported(String filePrefix) {
    return verifyGcsPath(filePrefix);
  }

  /**
   * Verifies that a path can be used by the Dataflow Service API.
   * @return the supplied path
   */
  @Override
  public String verifyGcsPath(String path) {
    GcsPath gcsPath = GcsPath.fromUri(path);
    Preconditions.checkArgument(gcsPath.isAbsolute(),
        "Must provide absolute paths for Dataflow");
    Preconditions.checkArgument(!gcsPath.getObject().contains("//"),
        "Dataflow Service does not allow objects with consecutive slashes");
    return gcsPath.toResourceName();
  }
}
