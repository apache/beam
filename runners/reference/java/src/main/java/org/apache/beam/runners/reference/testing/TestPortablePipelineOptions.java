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
package org.apache.beam.runners.reference.testing;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/** Options for {@link TestPortableRunner}. */
public interface TestPortablePipelineOptions extends TestPipelineOptions, PortablePipelineOptions {

  @Required
  @Description(
      "Fully qualified class name of TestJobServiceDriver capable of managing the JobService.")
  Class getJobServerDriver();

  void setJobServerDriver(Class jobServerDriver);

  @Description("String containing comma separated arguments for the JobServer.")
  @Default.InstanceFactory(DefaultJobServerConfigFactory.class)
  String[] getJobServerConfig();

  void setJobServerConfig(String... jobServerConfig);

  /** Factory for default config. */
  class DefaultJobServerConfigFactory implements DefaultValueFactory<String[]> {

    @Override
    public String[] create(PipelineOptions options) {
      return new String[0];
    }
  }

  /** Register {@link TestPortablePipelineOptions}. */
  @AutoService(PipelineOptionsRegistrar.class)
  class TestPortablePipelineOptionsRegistrar implements PipelineOptionsRegistrar {

    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return ImmutableList.of(TestPortablePipelineOptions.class);
    }
  }
}
