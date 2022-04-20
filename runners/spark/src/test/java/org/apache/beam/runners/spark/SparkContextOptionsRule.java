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

import javax.annotation.Nullable;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.values.KV;

public class SparkContextOptionsRule extends SparkContextRule {

  private Class<? extends PipelineRunner<?>> runner;
  private @Nullable SparkContextOptions contextOptions = null;

  public SparkContextOptionsRule(
      Class<? extends PipelineRunner<?>> runner, KV<String, String>... sparkConfig) {
    super(sparkConfig);
    this.runner = runner;
  }

  @Override
  protected void before() throws Throwable {
    super.before();
    contextOptions = createPipelineOptions(runner);
  }

  public SparkContextOptions getOptions() {
    if (contextOptions == null) {
      throw new IllegalStateException("SparkContextOptions not available");
    }
    return contextOptions;
  }
}
