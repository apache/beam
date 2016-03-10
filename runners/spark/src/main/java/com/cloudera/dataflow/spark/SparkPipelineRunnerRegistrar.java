/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineRunnerRegistrar;
import com.google.common.collect.ImmutableList;

public class SparkPipelineRunnerRegistrar implements PipelineRunnerRegistrar {
  @Override
  public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
    return ImmutableList.<Class<? extends PipelineRunner<?>>>of(SparkPipelineRunner.class);
  }
}
