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
package org.apache.beam.runners.fnexecution.wire;

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;

import java.util.function.Predicate;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardCoders;
import org.apache.beam.runners.core.construction.SyntheticComponents;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;

/** Helpers to construct coders for gRPC port reads and writes. */
public class WireCoders {
  /** Create an SDK-side wire coder for a port read/write for the given PCollection. */
  public static MessageWithComponents createSdkWireCoder(
      PCollectionNode pCollectionNode, Components components, Predicate<String> idUsed) {
    return createWireCoder(pCollectionNode, components, idUsed, false);
  }

  /**
   * Create a runner-side wire coder for a port read/write for the given PCollection. Returns a
   * windowed value coder. The element coder itself
   */
  public static MessageWithComponents createRunnerWireCoder(
      PCollectionNode pCollectionNode, Components components, Predicate<String> idUsed) {
    return createWireCoder(pCollectionNode, components, idUsed, true);
  }

  private static MessageWithComponents createWireCoder(
      PCollectionNode pCollectionNode,
      Components components,
      Predicate<String> idUsed,
      boolean useByteArrayCoder) {
    String elementCoderId = pCollectionNode.getPCollection().getCoderId();
    String windowingStrategyId = pCollectionNode.getPCollection().getWindowingStrategyId();
    String windowCoderId =
        components.getWindowingStrategiesOrThrow(windowingStrategyId).getWindowCoderId();
    Coder windowedValueCoder =
        Coder.newBuilder()
            .addComponentCoderIds(elementCoderId)
            .addComponentCoderIds(windowCoderId)
            .setSpec(
                SdkFunctionSpec.newBuilder()
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(getUrn(StandardCoders.Enum.WINDOWED_VALUE))))
            .build();
    // Add the original WindowedValue<T, W> coder to the components;
    String windowedValueId =
        SyntheticComponents.uniqueId(String.format("fn/wire/%s", pCollectionNode.getId()), idUsed);
    return LengthPrefixUnknownCoders.forCoder(
        windowedValueId,
        components.toBuilder().putCoders(windowedValueId, windowedValueCoder).build(),
        useByteArrayCoder);
  }

  // Not instantiable.
  private WireCoders() {}
}
