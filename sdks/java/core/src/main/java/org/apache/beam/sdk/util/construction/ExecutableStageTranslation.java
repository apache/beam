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
package org.apache.beam.sdk.util.construction;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.LinkedHashMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;

/**
 * Utilities for converting {@link ExecutableStage}s to and from {@link RunnerApi} protocol buffers.
 */
public class ExecutableStageTranslation {

  /** Extracts an {@link ExecutableStagePayload} from the given transform. */
  public static ExecutableStagePayload getExecutableStagePayload(
      AppliedPTransform<?, ?, ?> appliedTransform) throws IOException {
    RunnerApi.PTransform transform =
        PTransformTranslation.toProto(
            appliedTransform, SdkComponents.create(appliedTransform.getPipeline().getOptions()));
    checkArgument(ExecutableStage.URN.equals(transform.getSpec().getUrn()));
    return ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());
  }

  public static String generateNameFromStagePayload(ExecutableStagePayload stagePayload) {
    StringBuilder sb = new StringBuilder();
    RunnerApi.Components components = stagePayload.getComponents();
    final int transformsCount = stagePayload.getTransformsCount();
    sb.append("[").append(transformsCount).append("]");
    Collection<String> names = new ArrayList<>();
    for (int i = 0; i < transformsCount; i++) {
      String name = components.getTransformsOrThrow(stagePayload.getTransforms(i)).getUniqueName();
      // Java: Remove the 'ParMultiDo(Anonymous)' suffix which just makes the name longer
      name = name.replaceFirst("/ParMultiDo\\(Anonymous\\)$", "");
      names.add(name);
    }
    sb.append(generateNameFromTransformNames(names, true));
    return sb.toString();
  }

  /**
   * Creates a human-readable name for a set of stage names that occur in a single stage.
   *
   * <p>This name reflects the nested structure of the stages, as inferred by slashes in the stage
   * names. Sibling stages will be listed as {A, B}, nested stages as A/B, and according to the
   * value of truncateSiblingComposites the nesting stops at the first level that siblings are
   * encountered.
   *
   * <p>This is best understood via examples, of which there are several in the tests for this
   * class.
   *
   * @param names a list of full stage names in this fused operation
   * @param truncateSiblingComposites whether to recursively descent into composite operations that
   *     have simblings, or stop the recursion at that level.
   * @return a single string representation of all the stages in this fused operation
   */
  public static String generateNameFromTransformNames(
      Collection<String> names, boolean truncateSiblingComposites) {
    Multimap<String, String> groupByOuter = LinkedHashMultimap.create();
    for (String name : names) {
      int index = name.indexOf('/');
      if (index == -1) {
        groupByOuter.put(name, "");
      } else {
        groupByOuter.put(name.substring(0, index), name.substring(index + 1));
      }
    }
    if (groupByOuter.keySet().size() == 1) {
      Map.Entry<String, Collection<String>> outer =
          Iterables.getOnlyElement(groupByOuter.asMap().entrySet());
      if (outer.getValue().size() == 1 && outer.getValue().contains("")) {
        // Names consisted of a single name without any slashes.
        return outer.getKey();
      } else {
        // Everything is in the same outer stage, enumerate at one level down.
        return String.format(
            "%s/%s",
            outer.getKey(),
            generateNameFromTransformNames(outer.getValue(), truncateSiblingComposites));
      }
    } else {
      Collection<String> parts;
      if (truncateSiblingComposites) {
        // Enumerate the outer stages without their composite structure, if any.
        parts = groupByOuter.keySet();
      } else {
        // Enumerate the outer stages with their composite structure, if any.
        parts =
            groupByOuter.asMap().entrySet().stream()
                .map(
                    outer ->
                        String.format(
                                "%s/%s",
                                outer.getKey(),
                                generateNameFromTransformNames(
                                    outer.getValue(), truncateSiblingComposites))
                            .replaceAll("/$", ""))
                .collect(Collectors.toList());
      }
      return String.format("{%s}", Joiner.on(", ").join(parts));
    }
  }
}
