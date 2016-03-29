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

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.common.base.Preconditions;

import java.io.IOException;

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

  /**
   * Validates the the input GCS path is accessible and that the path
   * is well formed.
   */
  @Override
  public String validateInputFilePatternSupported(String filepattern) {
    GcsPath gcsPath = getGcsPath(filepattern);
    Preconditions.checkArgument(
        dataflowOptions.getGcsUtil().isGcsPatternSupported(gcsPath.getObject()));
    String returnValue = verifyPath(filepattern);
    verifyPathIsAccessible(filepattern, "Could not find file %s");
    return returnValue;
  }

  /**
   * Validates the the output GCS path is accessible and that the path
   * is well formed.
   */
  @Override
  public String validateOutputFilePrefixSupported(String filePrefix) {
    String returnValue = verifyPath(filePrefix);
    verifyPathIsAccessible(filePrefix, "Output path does not exist or is not writeable: %s");
    return returnValue;
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

  private void verifyPathIsAccessible(String path, String errorMessage) {
    GcsPath gcsPath = getGcsPath(path);
    try {
      Preconditions.checkArgument(dataflowOptions.getGcsUtil().bucketExists(gcsPath),
        errorMessage, path);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Unable to verify that GCS bucket gs://%s exists.", gcsPath.getBucket()),
          e);
    }
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
