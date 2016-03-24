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

import com.google.cloud.dataflow.sdk.options.PipelineOptions;

/**
 * Noop implementation of {@link PathValidator}. All paths are allowed and returned unchanged.
 */
public class NoopPathValidator implements PathValidator {

  private NoopPathValidator() {
  }

  public static PathValidator fromOptions(
      @SuppressWarnings("unused") PipelineOptions options) {
    return new NoopPathValidator();
  }

  @Override
  public String validateInputFilePatternSupported(String filepattern) {
    return filepattern;
  }

  @Override
  public String validateOutputFilePrefixSupported(String filePrefix) {
    return filePrefix;
  }

  @Override
  public String verifyPath(String path) {
    return path;
  }
}
