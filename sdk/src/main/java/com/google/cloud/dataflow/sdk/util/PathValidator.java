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

import com.google.cloud.dataflow.sdk.options.PipelineOptions;

/**
 * Interface for controlling validation of paths.
 */
public interface PathValidator {
  /**
   * Validates paths in the current {@link PipelineOptions} object. May modify the
   * options object.
   */
  public void validateAndUpdateOptions();

  /**
   * Validate that a file pattern is conforming.
   *
   * @param filepattern The file pattern to verify.
   * @return The post-validation filepattern.
   */
  public String validateInputFilePatternSupported(String filepattern);

  /**
   * Validate that an output file prefix is conforming.
   *
   * @param filePrefix the file prefix to verify.
   * @return The post-validation filePrefix.
   */
  public String validateOutputFilePrefixSupported(String filePrefix);

  /**
   * Validate that a GCS path is conforming.
   *
   * @param path The GCS path to verify.
   * @return The post-validation path.
   */
  public String verifyGcsPath(String path);
}
