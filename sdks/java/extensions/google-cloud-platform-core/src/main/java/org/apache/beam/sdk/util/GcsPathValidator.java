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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.fs.PathValidator;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.gcsfs.GcsPath;

/**
 * GCP implementation of {@link PathValidator}. Only GCS paths are allowed.
 */
public class GcsPathValidator implements PathValidator {

  private GcsOptions gcpOptions;

  private GcsPathValidator(GcsOptions options) {
    this.gcpOptions = options;
  }

  public static GcsPathValidator fromOptions(PipelineOptions options) {
    return new GcsPathValidator(options.as(GcsOptions.class));
  }

  /**
   * Validates the the input GCS path is accessible and that the path
   * is well formed.
   */
  @Override
  public void validateInputFilePatternSupported(String filepattern) {
    GcsPath gcsPath = getGcsPath(filepattern);
    checkArgument(GcsUtil.isGcsPatternSupported(gcsPath.getObject()));
    verifyPath(filepattern);
    verifyPathIsAccessible(filepattern, "Could not find file %s");
  }

  /**
   * Validates the the output GCS path is accessible and that the path
   * is well formed.
   */
  @Override
  public void validateOutputFilePrefixSupported(String filePrefix) {
    verifyPath(filePrefix);
    verifyPathIsAccessible(filePrefix, "Output path does not exist or is not writeable: %s");
  }

  @Override
  public void validateOutputResourceSupported(ResourceId resourceId) {
    checkArgument(
        resourceId.getScheme().equals("gs"),
        "Expected a valid 'gs://' path but was given: '%s'",
        resourceId);
    verifyPath(resourceId.toString());
  }

  @Override
  public String verifyPath(String path) {
    GcsPath gcsPath = getGcsPath(path);
    checkArgument(gcsPath.isAbsolute(), "Must provide absolute paths for Dataflow");
    checkArgument(!gcsPath.getObject().isEmpty(),
        "Missing object or bucket in path: '%s', did you mean: 'gs://some-bucket/%s'?",
        gcsPath, gcsPath.getBucket());
    checkArgument(!gcsPath.getObject().contains("//"),
        "Dataflow Service does not allow objects with consecutive slashes");
    return gcsPath.toResourceName();
  }

  private void verifyPathIsAccessible(String path, String errorMessage) {
    GcsPath gcsPath = getGcsPath(path);
    try {
      checkArgument(gcpOptions.getGcsUtil().bucketAccessible(gcsPath),
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
          "Expected a valid 'gs://' path but was given '%s'", path), e);
    }
  }
}
