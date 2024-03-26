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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import com.google.api.services.dataflow.model.MapTask;
import java.util.Random;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.apiary.FixMultiOutputInfosOnParDoInstructions;
import org.apache.beam.runners.dataflow.worker.graph.Edges;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.joda.time.Duration;

public final class StreamingEnvironment {
  /* The idGenerator to generate unique id globally. */
  private static final IdGenerator ID_GENERATOR = IdGenerators.decrementingLongs();
  private static final Random CLIENT_ID_GENERATOR = new Random();

  /**
   * Fix up MapTask representation because MultiOutputInfos are missing from system generated
   * ParDoInstructions.
   */
  private static final Function<MapTask, MapTask> FIX_MULTI_OUTPUT_INFOS =
      new FixMultiOutputInfosOnParDoInstructions(ID_GENERATOR);

  /**
   * Function which converts map tasks to their network representation for execution.
   *
   * <ul>
   *   <li>Translate the map task to a network representation.
   *   <li>Remove flatten instructions by rewiring edges.
   * </ul>
   */
  private static final Function<MapTask, MutableNetwork<Nodes.Node, Edges.Edge>>
      MAP_TASK_TO_BASE_NETWORK = new MapTaskToNetworkFunction(ID_GENERATOR);

  static IdGenerator idGeneratorInstance() {
    return ID_GENERATOR;
  }

  public static Function<MapTask, MapTask> fixMapTaskMultiOutputInfoFnInstance() {
    return FIX_MULTI_OUTPUT_INFOS;
  }

  static Function<MapTask, MutableNetwork<Nodes.Node, Edges.Edge>>
      mapTaskToBaseNetworkFnInstance() {
    return MAP_TASK_TO_BASE_NETWORK;
  }

  static Duration localHostMaxBackoff() {
    return Duration.millis(500);
  }

  static long newClientId() {
    return CLIENT_ID_GENERATOR.nextLong();
  }
}
