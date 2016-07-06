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

package org.apache.beam.runners.spark;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * The SparkRunner translate operations defined on a pipeline to a representation executable
 * by Spark, and then submitting the job to Spark to be executed. If we wanted to run a dataflow
 * pipeline with the default options of a single threaded spark instance in local mode, we would do
 * the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * EvaluationResult result = SparkRunner.create().run(p);
 * }
 *
 * To create a pipeline runner to run against a different spark cluster, with a custom master url we
 * would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
 * options.setSparkMaster("spark://host:port");
 * EvaluationResult result = SparkRunner.create(options).run(p);
 * }
 *
 * To create a Spark streaming pipeline runner use {@link SparkStreamingPipelineOptions}
 */
public final class TestSparkRunner extends PipelineRunner<EvaluationResult> {

  private SparkRunner delegate;

  private TestSparkRunner(SparkPipelineOptions options) {
    this.delegate = SparkRunner.fromOptions(options);
  }

  public static TestSparkRunner fromOptions(PipelineOptions options) {
    // Default options suffice to set it up as a test runner
    SparkPipelineOptions sparkOptions =
        PipelineOptionsValidator.validate(SparkPipelineOptions.class, options);
    return new TestSparkRunner(sparkOptions);
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput>
      OutputT apply(PTransform<InputT, OutputT> transform, InputT input) {
    return delegate.apply(transform, input);
  };

  @Override
  public EvaluationResult run(Pipeline pipeline) {
    return delegate.run(pipeline);
  }
}
