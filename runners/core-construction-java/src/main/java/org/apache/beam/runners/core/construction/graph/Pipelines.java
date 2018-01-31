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

package org.apache.beam.runners.core.construction.graph;

import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;

/**
 * Utilities for interacting with {@link Pipeline Pipelines} and {@link Components}.
 */
public class Pipelines {
  private Pipelines() {}

  /**
   * Produces a {@link RunnerApi.Components} which contains only primitive transforms.
   */
  public static RunnerApi.Components retainOnlyPrimitives(RunnerApi.Components components) {
    RunnerApi.Components.Builder flattenedBuilder = components.toBuilder();
    flattenedBuilder.clearTransforms();
    for (Map.Entry<String, PTransform> transformEntry : components.getTransformsMap().entrySet()) {
      PTransform transform = transformEntry.getValue();
      boolean isPrimitive = isPrimitiveTransform(transform);
      if (isPrimitive) {
        flattenedBuilder.putTransforms(transformEntry.getKey(), transform);
      }
    }
    return flattenedBuilder.build();
  }

  /**
   * Returns true if the provided transform is a primitive. A primitive has no subtransforms and
   * produces a new {@link PCollection}.
   */
  private static boolean isPrimitiveTransform(PTransform transform) {
    return transform.getSubtransformsCount() == 0
        && !transform.getInputsMap().values().containsAll(transform.getOutputsMap().values());
  }
}
