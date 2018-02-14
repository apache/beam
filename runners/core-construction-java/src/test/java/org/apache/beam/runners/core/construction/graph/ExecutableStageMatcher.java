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

import static org.hamcrest.Matchers.containsInAnyOrder;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * A {@link Matcher} that matches the input and output IDs of an {@link ExecutableStage} and the IDs
 * of all of the {@link PTransform PTransforms} it contains.
 */
public class ExecutableStageMatcher extends TypeSafeMatcher<ExecutableStage> {
  private final String inputPCollectionId;
  private final Collection<String> materializedPCollection;
  private final Collection<String> fusedTransforms;

  private ExecutableStageMatcher(
      String inputPCollectionId,
      Collection<String> materializedPCollection,
      Collection<String> fusedTransforms) {
    this.inputPCollectionId = inputPCollectionId;
    this.materializedPCollection = materializedPCollection;
    this.fusedTransforms = fusedTransforms;
  }

  public static ExecutableStageMatcher withInput(String inputId) {
    return new ExecutableStageMatcher(inputId, ImmutableList.of(), ImmutableList.of());
  }

  public ExecutableStageMatcher withNoOutputs() {
    return new ExecutableStageMatcher(inputPCollectionId, ImmutableList.of(), fusedTransforms);
  }

  public ExecutableStageMatcher withOutputs(String... pCollections) {
    return new ExecutableStageMatcher(
        inputPCollectionId, ImmutableList.copyOf(pCollections), fusedTransforms);
  }

  public ExecutableStageMatcher withTransforms(String... transforms) {
    return new ExecutableStageMatcher(
        inputPCollectionId, materializedPCollection, ImmutableList.copyOf(transforms));
  }

  @Override
  protected boolean matchesSafely(ExecutableStage item) {
    return item.getInputPCollection().getId().equals(inputPCollectionId)
        && containsInAnyOrder(materializedPCollection.toArray(new String[0]))
            .matches(
                item.getOutputPCollections()
                    .stream()
                    .map(PCollectionNode::getId)
                    .collect(Collectors.toSet()))
        && containsInAnyOrder(fusedTransforms.toArray(new String[0]))
            .matches(
                item.getTransforms()
                    .stream()
                    .map(PTransformNode::getId)
                    .collect(Collectors.toSet()));
  }

  @Override
  public void describeTo(Description description) {
    description
        .appendText(
            String.format(
                "An %s with input %s ",
                ExecutableStage.class.getSimpleName(), PCollection.class.getSimpleName()))
        .appendText(inputPCollectionId)
        .appendText(String.format(", output %ss ", PCollection.class.getSimpleName()))
        .appendValueList("[", ", ", "]", materializedPCollection)
        .appendText(String.format(" and fused %ss ", PTransform.class.getSimpleName()))
        .appendValueList("[", ", ", "]", fusedTransforms);
  }
}
