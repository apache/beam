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
package org.apache.beam.runners.jstorm;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * {@link PipelineOptions} that configures the JStorm pipeline.
 */
public interface JStormPipelineOptions extends PipelineOptions {

  @Description("Indicate if the topology is running on local machine or distributed cluster")
  @Default.Boolean(false)
  Boolean getLocalMode();
  void setLocalMode(Boolean isLocal);

  @Description("Executing time(sec) of topology on local mode. Default is 1min.")
  @Default.Long(60)
  Long getLocalModeExecuteTime();
  void setLocalModeExecuteTime(Long time);

  @Description("Worker number of topology")
  @Default.Integer(1)
  Integer getWorkerNumber();
  void setWorkerNumber(Integer number);

  @Description("Global parallelism number of a component")
  @Default.Integer(1)
  Integer getParallelismNumber();
  void setParallelismNumber(Integer number);

  @Description("System topology config of JStorm")
  @Default.InstanceFactory(DefaultMapValueFactory.class)
  Map getTopologyConfig();
  void setTopologyConfig(Map conf);

  @Description("Indicate if it is an exactly once topology")
  @Default.Boolean(false)
  Boolean getExactlyOnceTopology();
  void setExactlyOnceTopology(Boolean isExactlyOnce);

  @Description("Parallelism number of a specified composite PTransform")
  @Default.InstanceFactory(DefaultMapValueFactory.class)
  Map getParallelismNumMap();
  void setParallelismNumMap(Map parallelismNumMap);

  /**
   * Default value factory for topology configuration of JStorm.
   */
  class DefaultMapValueFactory implements DefaultValueFactory<Map> {
    @Override
    public Map create(PipelineOptions pipelineOptions) {
      return Maps.newHashMap();
    }
  }
}
