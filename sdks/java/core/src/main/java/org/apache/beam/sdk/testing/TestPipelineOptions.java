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
package org.apache.beam.sdk.testing;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

/**
 * {@link TestPipelineOptions} is a set of options for test pipelines.
 *
 * @see TestPipeline
 */
public interface TestPipelineOptions extends PipelineOptions {
  String getTempRoot();
  void setTempRoot(String value);

  @Default.InstanceFactory(AlwaysPassMatcherFactory.class)
  Matcher<PipelineResult> getOnCreateMatcher();
  void setOnCreateMatcher(Matcher<PipelineResult> value);

  @Default.InstanceFactory(AlwaysPassMatcherFactory.class)
  Matcher<PipelineResult> getOnSuccessMatcher();
  void setOnSuccessMatcher(Matcher<PipelineResult> value);

  /**
   * Factory for PipelineResult matchers which always pass.
   */
  class AlwaysPassMatcherFactory
      implements DefaultValueFactory<Matcher<PipelineResult>> {
    @Override
    public Matcher<PipelineResult> create(PipelineOptions options) {
      return Matchers.any(PipelineResult.class);
    }
  }

}
