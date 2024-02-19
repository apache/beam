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

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.ProjectionProducer;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * {@link PipelineVisitor} to convert projection pushdown targets from {@link PCollection} to {@link
 * TupleTag}.
 *
 * <p>For example, if we can do pushdown on {@link PTransform} {@code T}'s output {@link
 * PCollection} {@code P} by rewriting {@code T}, we need to get {@code T}'s output tag for {@code
 * P}. This is necessary because {@link PCollection} objects are not instantiated until pipeline
 * construction, but output tags are constants that are known before pipeline construction, so
 * transform authors can identify them in {@link ProjectionProducer#actuateProjectionPushdown(Map)}.
 */
class PCollectionOutputTagVisitor extends PipelineVisitor.Defaults {
  private final Map<
          ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
      pCollFieldAccess;
  private final ImmutableMap.Builder<
          ProjectionProducer<PTransform<?, ?>>,
          ImmutableMap.Builder<TupleTag<?>, FieldAccessDescriptor>>
      tagFieldAccess = ImmutableMap.builder();

  PCollectionOutputTagVisitor(
      Map<ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
          pCollFieldAccess) {
    this.pCollFieldAccess = pCollFieldAccess;
  }

  Map<ProjectionProducer<PTransform<?, ?>>, Map<TupleTag<?>, FieldAccessDescriptor>>
      getTaggedFieldAccess() {
    return tagFieldAccess.build().entrySet().stream()
        .map(e -> new SimpleEntry<>(e.getKey(), e.getValue().build()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  @Override
  public void visitValue(PValue value, Node producer) {
    for (Entry<ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
        entry : pCollFieldAccess.entrySet()) {
      FieldAccessDescriptor fieldAccess = entry.getValue().get(value);
      if (fieldAccess == null) {
        continue;
      }
      BiMap<PCollection<?>, TupleTag<?>> outputs =
          ImmutableBiMap.copyOf(producer.getOutputs()).inverse();
      TupleTag<?> tag = outputs.get(value);
      Preconditions.checkArgumentNotNull(
          tag, "PCollection %s not found in outputs of producer %s", value, producer);
      ImmutableMap.Builder<TupleTag<?>, FieldAccessDescriptor> tagEntryBuilder =
          tagFieldAccess.build().get(entry.getKey());
      if (tagEntryBuilder == null) {
        tagEntryBuilder = ImmutableMap.builder();
        tagFieldAccess.put(entry.getKey(), tagEntryBuilder);
      }
      tagEntryBuilder.put(tag, fieldAccess);
    }
  }
}
