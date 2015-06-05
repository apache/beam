/*
 * Copyright (C) 2015 Google Inc.
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

/**
 * GCP implementation of {@link PathValidator}. Only GCS paths are allowed.
 */
public class DataflowPathValidator implements PathValidator {

  private DataflowPipelineOptions dataflowOptions;

  DataflowPathValidator(DataflowPipelineOptions options) {
    this.dataflowOptions = options;
  }

  public static DataflowPathValidator fromOptions(PipelineOptions options) {
    return new DataflowPathValidator(options.as(DataflowPipelineOptions.class));
  }

  @Override
  public String validateInputFilePatternSupported(String filepattern) {
    GcsPath gcsPath = getGcsPath(filepattern);
    Preconditions.checkArgument(
        dataflowOptions.getGcsUtil().isGcsPatternSupported(gcsPath.getObject()));
    return verifyPath(filepattern);
  }

  @Override
  public String validateOutputFilePrefixSupported(String filePrefix) {
    return verifyPath(filePrefix);
  }

  @Override
  public String verifyPath(String path) {
    GcsPath gcsPath = getGcsPath(path);
    Preconditions.checkArgument(gcsPath.isAbsolute(),
        "Must provide absolute paths for Dataflow");
    Preconditions.checkArgument(!gcsPath.getObject().contains("//"),
        "Dataflow Service does not allow objects with consecutive slashes");
    return gcsPath.toResourceName();
  }

  private GcsPath getGcsPath(String path) {
    try {
      return GcsPath.fromUri(path);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format(
          "%s expected a valid 'gs://' path but was given '%s'",
          dataflowOptions.getRunner().getSimpleName(), path), e);
    }
  }
}
