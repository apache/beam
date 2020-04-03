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
package org.apache.beam.runners.core.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;

/**
 * A Class for registering SimpleExecutionStates with and extracting execution time MonitoringInfos.
 */
public class SimpleStateRegistry {
  private List<SimpleExecutionState> executionStates = new ArrayList<SimpleExecutionState>();

  public void register(SimpleExecutionState state) {
    this.executionStates.add(state);
  }

  /** Reset the registered SimpleExecutionStates. */
  public void reset() {
    for (SimpleExecutionState state : executionStates) {
      state.reset();
    }
  }

  /** @return Execution Time MonitoringInfos based on the tracked start or finish function. */
  public List<MonitoringInfo> getExecutionTimeMonitoringInfos() {
    List<MonitoringInfo> monitoringInfos = new ArrayList<MonitoringInfo>();
    for (SimpleExecutionState state : executionStates) {
      SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
      builder.setUrn(state.getUrn());
      for (Map.Entry<String, String> entry : state.getLabels().entrySet()) {
        builder.setLabel(entry.getKey(), entry.getValue());
      }
      builder.setInt64SumValue(state.getTotalMillis());
      monitoringInfos.add(builder.build());
    }
    return monitoringInfos;
  }
}
