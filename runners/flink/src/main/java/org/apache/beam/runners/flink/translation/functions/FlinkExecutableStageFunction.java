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
package org.apache.beam.runners.flink.translation.functions;

import com.google.protobuf.Struct;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// TODO: https://issues.apache.org/jira/browse/BEAM-2597 Implement this executable stage operator.
/**
 * Flink operator that passes its input DataSet through an SDK-executed {@link
 * org.apache.beam.runners.core.construction.graph.ExecutableStage}.
 *
 * <p>The output of this operation is a multiplexed DataSet whose elements are tagged with a union
 * coder. The coder's tags are determined by the output coder map. The resulting data set should be
 * further processed by a {@link FlinkExecutableStagePruningFunction}.
 */
public class FlinkExecutableStageFunction<InputT>
    extends RichMapPartitionFunction<WindowedValue<InputT>, RawUnionValue> {

  // The executable stage this function will run.
  private final RunnerApi.ExecutableStagePayload stagePayload;
  // Pipeline options. Used for provisioning api.
  private final Struct pipelineOptions;
  // Map from PCollection id to the union tag used to represent this PCollection in the output.
  private final Map<String, Integer> outputMap;

  public FlinkExecutableStageFunction(
      RunnerApi.ExecutableStagePayload stagePayload,
      Struct pipelineOptions,
      Map<String, Integer> outputMap) {
    this.stagePayload = stagePayload;
    this.pipelineOptions = pipelineOptions;
    this.outputMap = outputMap;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mapPartition(
      Iterable<WindowedValue<InputT>> iterable, Collector<RawUnionValue> collector)
      throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws Exception {
    throw new UnsupportedOperationException();
  }
}
