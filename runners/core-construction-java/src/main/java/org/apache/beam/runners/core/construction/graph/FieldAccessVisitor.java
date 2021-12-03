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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Computes which Schema fields are (or conversely, are not) accessed in a pipeline. */
class FieldAccessVisitor extends PipelineVisitor.Defaults {
  private final Map<String, FieldAccessDescriptor> pCollectionFieldAccess = new HashMap<>();

  /**
   * Returns a map keyed by the {@link TupleTag} ID referencing a PCollection. Values are the set of
   * fields accessed on that PCollection.
   */
  ImmutableMap<String, FieldAccessDescriptor> getPCollectionFieldAccess() {
    return ImmutableMap.copyOf(pCollectionFieldAccess);
  }

  @Override
  public void visitPrimitiveTransform(Node node) {
    Map<String, FieldAccessDescriptor> currentFieldAccess = getFieldAccess(node);
    for (Entry<String, FieldAccessDescriptor> entry : currentFieldAccess.entrySet()) {
      FieldAccessDescriptor previousFieldAccess = pCollectionFieldAccess.get(entry.getKey());
      FieldAccessDescriptor newFieldAccess =
          previousFieldAccess == null
              ? entry.getValue()
              : FieldAccessDescriptor.union(
                  ImmutableList.of(previousFieldAccess, entry.getValue()));
      pCollectionFieldAccess.put(entry.getKey(), newFieldAccess);
    }
  }

  private static Map<String, FieldAccessDescriptor> getFieldAccess(Node node) {
    PTransform<?, ?> transform = node.getTransform();
    HashMap<String, FieldAccessDescriptor> access = new HashMap<>();

    if (transform instanceof MultiOutput) {
      DoFn<?, ?> fn = ((MultiOutput<?, ?>) transform).getFn();
      Pair<TupleTag<?>, PCollection<?>> mainInput = getMainInputTagId(node);
      FieldAccessDescriptor fields =
          ParDo.getDoFnSchemaInformation(fn, mainInput.getRight()).getFieldAccessDescriptor();
      access.put(mainInput.getLeft().getId(), fields);
    }

    // For every input without field access info, we must assume all fields need to be accessed.
    for (TupleTag<?> tag : node.getInputs().keySet()) {
      if (!access.containsKey(tag.getId())) {
        access.put(tag.getId(), FieldAccessDescriptor.withAllFields());
      }
    }

    return ImmutableMap.copyOf(access);
  }

  private static Pair<TupleTag<?>, PCollection<?>> getMainInputTagId(Node node) {
    HashSet<TupleTag<?>> mainInputTags = new HashSet<>(node.getInputs().keySet());
    Map<TupleTag<?>, PValue> additionalInputs = node.getTransform().getAdditionalInputs();
    if (additionalInputs != null) {
      mainInputTags.removeAll(additionalInputs.keySet());
    }
    TupleTag<?> mainInputTag = Iterables.getOnlyElement(mainInputTags);
    PCollection<?> pCollection = node.getInputs().get(mainInputTag);
    Preconditions.checkNotNull(pCollection);
    return Pair.of(mainInputTag, pCollection);
  }
}
