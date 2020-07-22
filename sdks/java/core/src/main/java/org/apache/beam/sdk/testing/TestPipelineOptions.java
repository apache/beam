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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

/**
 * {@link TestPipelineOptions} is a set of options for test pipelines.
 *
 * @see TestPipeline
 */
public interface TestPipelineOptions extends PipelineOptions {
  String getTempRoot();

  void setTempRoot(String value);

  @Default.InstanceFactory(AlwaysPassMatcherFactory.class)
  @JsonIgnore
  SerializableMatcher<PipelineResult> getOnCreateMatcher();

  void setOnCreateMatcher(SerializableMatcher<PipelineResult> value);

  @Default.InstanceFactory(AlwaysPassMatcherFactory.class)
  @JsonIgnore
  SerializableMatcher<PipelineResult> getOnSuccessMatcher();

  void setOnSuccessMatcher(SerializableMatcher<PipelineResult> value);

  @Default.Long(10 * 60)
  @Nullable
  Long getTestTimeoutSeconds();

  void setTestTimeoutSeconds(Long value);

  /** Factory for {@link PipelineResult} matchers which always pass. */
  class AlwaysPassMatcherFactory
      implements DefaultValueFactory<SerializableMatcher<PipelineResult>> {
    @Override
    public SerializableMatcher<PipelineResult> create(PipelineOptions options) {
      return new AlwaysPassMatcher();
    }
  }

  /** Matcher which will always pass. */
  class AlwaysPassMatcher extends BaseMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {
    @Override
    public boolean matches(Object o) {
      return true;
    }

    @Override
    public void describeTo(Description description) {}
  }
}
