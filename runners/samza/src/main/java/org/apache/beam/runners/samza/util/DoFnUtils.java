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

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.commons.collections.CollectionUtils;

/** Utils for {@link org.apache.beam.runners.samza.runtime.DoFnOp}. */
public class DoFnUtils {

  public static String toStepName(ExecutableStage executableStage) {
    /*
     * Look for the first/input ParDo/DoFn in this executable stage by
     * matching ParDo/DoFn's input PCollection with executable stage's
     * input PCollection
     */
    Set<PipelineNode.PTransformNode> inputs =
        executableStage.getTransforms().stream()
            .filter(
                transform ->
                    transform
                        .getTransform()
                        .getInputsMap()
                        .containsValue(executableStage.getInputPCollection().getId()))
            .collect(Collectors.toSet());

    Set<String> outputIds =
        executableStage.getOutputPCollections().stream()
            .map(PipelineNode.PCollectionNode::getId)
            .collect(Collectors.toSet());

    /*
     * Look for the last/output ParDo/DoFn in this executable stage by
     * matching ParDo/DoFn's output PCollection(s) with executable stage's
     * out PCollection(s)
     */
    Set<PipelineNode.PTransformNode> outputs =
        executableStage.getTransforms().stream()
            .filter(
                transform ->
                    CollectionUtils.containsAny(
                        transform.getTransform().getOutputsMap().values(), outputIds))
            .collect(Collectors.toSet());

    return String.format("[%s-%s]", toStepName(inputs), toStepName(outputs));
  }

  private static String toStepName(Set<PipelineNode.PTransformNode> nodes) {
    // TODO: format name when there are multiple input/output PTransform(s) in the ExecutableStage
    return nodes.isEmpty()
        ? ""
        : Iterables.get(
            Splitter.on('/').split(nodes.iterator().next().getTransform().getUniqueName()), 0);
  }
}
