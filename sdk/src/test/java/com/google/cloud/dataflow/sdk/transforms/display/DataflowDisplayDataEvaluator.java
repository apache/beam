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

package com.google.cloud.dataflow.sdk.transforms.display;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineDebugOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.util.NoopCredentialFactory;
import com.google.cloud.dataflow.sdk.util.NoopPathValidator;
import com.google.common.collect.Lists;

/**
 * Factory methods for creating {@link DisplayDataEvaluator} instances against the
 * {@link DataflowPipelineRunner}.
 */
public final class DataflowDisplayDataEvaluator {
  /** Do not instantiate. */
  private DataflowDisplayDataEvaluator() {}

  /**
   * Retrieve a set of default {@link DataflowPipelineOptions} which can be used to build
   * dataflow pipelines for evaluating display data.
   */
  public static DataflowPipelineOptions getDefaultOptions() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

    options.setRunner(DataflowPipelineRunner.class);
    options.setProject("foobar");
    options.setTempLocation("gs://bucket/tmpLocation");
    options.setFilesToStage(Lists.<String>newArrayList());

    options.as(DataflowPipelineDebugOptions.class).setPathValidatorClass(NoopPathValidator.class);
    options.as(GcpOptions.class).setCredentialFactoryClass(NoopCredentialFactory.class);

    return options;
  }

  /**
   * Create a {@link DisplayDataEvaluator} instance to evaluate pipeline display data against
   * the {@link DataflowPipelineRunner}.
   */
  public static DisplayDataEvaluator create() {
    return create(getDefaultOptions());
  }

  /**
   * Create a {@link DisplayDataEvaluator} instance to evaluate pipeline display data against
   * the {@link DataflowPipelineRunner} with the specified {@code options}.
   */
  public static DisplayDataEvaluator create(DataflowPipelineOptions options) {
    return DisplayDataEvaluator.create(options);
  }
}
