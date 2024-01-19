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
package org.apache.beam.sdk.transforms.resourcehints;

import com.google.auto.service.AutoService;
import java.util.List;
import javax.lang.model.type.NullType;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Options that are used to control configuration of the remote environment. */
public interface ResourceHintsOptions extends PipelineOptions {
  class EmptyListDefault implements DefaultValueFactory<List<NullType>> {
    @Override
    public List<NullType> create(PipelineOptions options) {
      return ImmutableList.of();
    }
  }

  @Description("Resource hints used for all transform execution environments.")
  @Default.InstanceFactory(EmptyListDefault.class)
  List<String> getResourceHints();

  void setResourceHints(List<String> value);

  /** Register the {@link ResourceHintsOptions}. */
  @AutoService(PipelineOptionsRegistrar.class)
  class Options implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return ImmutableList.of(ResourceHintsOptions.class);
    }
  }
}
