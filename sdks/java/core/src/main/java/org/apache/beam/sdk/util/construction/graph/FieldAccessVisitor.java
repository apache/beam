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
package org.apache.beam.sdk.util.construction.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/** Computes which Schema fields are (or conversely, are not) accessed in a pipeline. */
class FieldAccessVisitor extends PipelineVisitor.Defaults {
  private final Map<PCollection<?>, FieldAccessDescriptor> pCollectionFieldAccess = new HashMap<>();

  /** Returns a map from PCollection to fields accessed by that PCollection. */
  ImmutableMap<PCollection<?>, FieldAccessDescriptor> getPCollectionFieldAccess() {
    return ImmutableMap.copyOf(pCollectionFieldAccess);
  }

  @Override
  public void visitPrimitiveTransform(Node node) {
    Map<PCollection<?>, FieldAccessDescriptor> currentFieldAccess = getFieldAccess(node);
    for (Entry<PCollection<?>, FieldAccessDescriptor> entry : currentFieldAccess.entrySet()) {
      FieldAccessDescriptor previousFieldAccess = pCollectionFieldAccess.get(entry.getKey());
      FieldAccessDescriptor newFieldAccess =
          previousFieldAccess == null
              ? entry.getValue()
              : FieldAccessDescriptor.union(
                  ImmutableList.of(previousFieldAccess, entry.getValue()));
      pCollectionFieldAccess.put(entry.getKey(), newFieldAccess);
    }
  }

  private static Map<PCollection<?>, FieldAccessDescriptor> getFieldAccess(Node node) {
    PTransform<?, ?> transform = node.getTransform();
    HashMap<PCollection<?>, FieldAccessDescriptor> access = new HashMap<>();

    if (transform instanceof MultiOutput) {
      // Get main input pcoll.
      Set<PCollection<?>> mainInputs =
          node.getInputs().entrySet().stream()
              .filter((entry) -> !transform.getAdditionalInputs().containsKey(entry.getKey()))
              .map(Entry::getValue)
              .collect(Collectors.toSet());
      PCollection<?> mainInput = Iterables.getOnlyElement(mainInputs);

      // Get field access.
      DoFn<?, ?> fn = ((MultiOutput<?, ?>) transform).getFn();
      FieldAccessDescriptor fields =
          ParDo.getDoFnSchemaInformation(fn, mainInput).getFieldAccessDescriptor();

      // Record field access.
      access.put(mainInput, fields);
    }

    // For every input without field access info, we must assume all fields need to be accessed.
    for (PCollection<?> input : node.getInputs().values()) {
      if (!access.containsKey(input)) {
        access.put(input, FieldAccessDescriptor.withAllFields());
      }
    }

    return ImmutableMap.copyOf(access);
  }
}
