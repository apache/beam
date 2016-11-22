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
package org.apache.beam.runners.spark.translation.streaming.utils;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.rules.ExternalResource;

/**
 * A rule to create a common {@link SparkPipelineOptions} test options for spark-runner.
 */
public class SparkTestPipelineOptions extends ExternalResource {

  protected final SparkPipelineOptions options =
      PipelineOptionsFactory.as(SparkPipelineOptions.class);

  @Override
  protected void before() throws Throwable {
    options.setRunner(SparkRunner.class);
    options.setEnableSparkMetricSinks(false);
  }

  public SparkPipelineOptions getOptions() {
    return options;
  }
}
