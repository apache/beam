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
package org.apache.beam.runners.dataflow.transforms;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.util.NoopCredentialFactory;
import org.apache.beam.sdk.util.NoopPathValidator;

import com.google.common.collect.Lists;

/**
 * Factory methods for creating {@link DisplayDataEvaluator} instances against the
 * {@link DataflowRunner}.
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

    options.setRunner(DataflowRunner.class);
    options.setProject("foobar");
    options.setTempLocation("gs://bucket/tmpLocation");
    options.setFilesToStage(Lists.<String>newArrayList());

    options.as(DataflowPipelineDebugOptions.class).setPathValidatorClass(NoopPathValidator.class);
    options.as(GcpOptions.class).setCredentialFactoryClass(NoopCredentialFactory.class);

    return options;
  }

  /**
   * Create a {@link DisplayDataEvaluator} instance to evaluate pipeline display data against
   * the {@link DataflowRunner}.
   */
  public static DisplayDataEvaluator create() {
    return create(getDefaultOptions());
  }

  /**
   * Create a {@link DisplayDataEvaluator} instance to evaluate pipeline display data against
   * the {@link DataflowRunner} with the specified {@code options}.
   */
  public static DisplayDataEvaluator create(DataflowPipelineOptions options) {
    return DisplayDataEvaluator.create(options);
  }
}
