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
package org.apache.beam.runners.samza.util;

import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.values.WindowingStrategy;

/** Utils for window operations. */
public class WindowUtils {

  /** Get {@link WindowingStrategy} of given collection id from {@link RunnerApi.Components}. */
  public static WindowingStrategy<?, BoundedWindow> getWindowStrategy(
      String collectionId, RunnerApi.Components components) {
    RehydratedComponents rehydratedComponents = RehydratedComponents.forComponents(components);

    RunnerApi.WindowingStrategy windowingStrategyProto =
        components.getWindowingStrategiesOrThrow(
            components.getPcollectionsOrThrow(collectionId).getWindowingStrategyId());

    WindowingStrategy<?, ?> windowingStrategy;
    try {
      windowingStrategy =
          WindowingStrategyTranslation.fromProto(windowingStrategyProto, rehydratedComponents);
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format(
              "Unable to hydrate GroupByKey windowing strategy %s.", windowingStrategyProto),
          e);
    }

    @SuppressWarnings("unchecked")
    WindowingStrategy<?, BoundedWindow> ret =
        (WindowingStrategy<?, BoundedWindow>) windowingStrategy;
    return ret;
  }

  /**
   * Instantiate {@link WindowedValue.WindowedValueCoder} for given collection id from {@link
   * RunnerApi.Components}.
   */
  public static <T> WindowedValue.WindowedValueCoder<T> instantiateWindowedCoder(
      String collectionId, RunnerApi.Components components) {
    PipelineNode.PCollectionNode collectionNode =
        PipelineNode.pCollection(collectionId, components.getPcollectionsOrThrow(collectionId));
    try {
      return (WindowedValue.WindowedValueCoder<T>)
          WireCoders.<T>instantiateRunnerWireCoder(collectionNode, components);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
