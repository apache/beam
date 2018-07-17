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
package org.apache.beam.runners.flink;

import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;

/**
 * {@link DefaultValueFactory} for getting a default value for the parallelism option on {@link
 * FlinkPipelineOptions}.
 *
 * <p>This will return either the default value from {@link GlobalConfiguration} or {@code 1}. A
 * valid {@link GlobalConfiguration} is only available if the program is executed by the Flink run
 * scripts.
 */
public class DefaultParallelismFactory implements DefaultValueFactory<Integer> {
  @Override
  public Integer create(PipelineOptions options) {
    return GlobalConfiguration.loadConfiguration()
        .getInteger(ConfigConstants.DEFAULT_PARALLELISM_KEY, 1);
  }
}
