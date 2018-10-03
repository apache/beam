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

package org.apache.beam.runners.dataflow.worker;

import com.google.api.services.dataflow.model.MapTask;
import com.google.common.graph.MutableNetwork;
import java.util.function.Supplier;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.sdk.options.PipelineOptions;

/** Creates a {@link DataflowMapTaskExecutor} from a {@link MapTask} definition. */
public interface DataflowMapTaskExecutorFactory {

  /**
   * Creates a new {@link DataflowMapTaskExecutor} from the given {@link MapTask} definition using
   * the provided {@link ReaderFactory} as well as a wide variety of other contextual information.
   */
  DataflowMapTaskExecutor create(
      InstructionRequestHandler instructionRequestHandler,
      FnDataService beamFnDataService,
      Endpoints.ApiServiceDescriptor dataApiServiceDescriptor,
      StateDelegator beamFnStateDelegator,
      MutableNetwork<Node, Edge> network,
      PipelineOptions options,
      String stageName,
      ReaderFactory readerFactory,
      SinkFactory sinkFactory,
      DataflowExecutionContext<?> executionContext,
      CounterSet counterSet,
      Supplier<String> idGenerator);
}
