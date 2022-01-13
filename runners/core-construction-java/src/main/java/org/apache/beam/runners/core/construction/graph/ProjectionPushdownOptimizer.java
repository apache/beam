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

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.ProjectionProducer;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** TODO(ibzib) javadoc */
public class ProjectionPushdownOptimizer {

  /** Performs all known projection pushdown optimizations in-place on a Pipeline. */
  public static void optimize(Pipeline pipeline) {
    // Compute which Schema fields are (or conversely, are not) accessed in a pipeline.
    FieldAccessVisitor fieldAccessVisitor = new FieldAccessVisitor();
    pipeline.traverseTopologically(fieldAccessVisitor);

    // Find transforms in this pipeline which both: 1. support projection pushdown and 2. output
    // unused fields.
    ProjectionProducerVisitor pushdownProjectorVisitor =
        new ProjectionProducerVisitor(fieldAccessVisitor.getPCollectionFieldAccess());
    pipeline.traverseTopologically(pushdownProjectorVisitor);
    Map<ProjectionProducer<PTransform<?, ?>>, Map<PCollection<?>, FieldAccessDescriptor>>
        pushdownOpportunities = pushdownProjectorVisitor.getPushdownOpportunities();

    // Translate target PCollections to their output TupleTags.
    PCollectionOutputTagVisitor outputTagVisitor =
        new PCollectionOutputTagVisitor(pushdownOpportunities);
    pipeline.traverseTopologically(outputTagVisitor);
    Map<ProjectionProducer<PTransform<?, ?>>, Map<TupleTag<?>, FieldAccessDescriptor>>
        taggedFieldAccess = outputTagVisitor.getTaggedFieldAccess();

    // For each eligible transform, replace it with a modified transform that omits the unused
    // fields.
    for (Entry<ProjectionProducer<PTransform<?, ?>>, Map<TupleTag<?>, FieldAccessDescriptor>>
        entry : taggedFieldAccess.entrySet()) {
      PTransformMatcher matcher = application -> application.getTransform() == entry.getKey();
      PushdownOverrideFactory<?> overrideFactory = new PushdownOverrideFactory<>(entry.getValue());
      pipeline.replaceAll(ImmutableList.of(PTransformOverride.of(matcher, overrideFactory)));
    }
  }

  private static class PushdownOverrideFactory<
          TransformT extends PTransform<PBegin, PCollection<Row>>>
      implements PTransformOverrideFactory<PBegin, PCollection<Row>, TransformT> {
    private final Map<TupleTag<?>, FieldAccessDescriptor> fields;

    PushdownOverrideFactory(Map<TupleTag<?>, FieldAccessDescriptor> fields) {
      this.fields = fields;
    }

    @Override
    public PTransformReplacement<PBegin, PCollection<Row>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<Row>, TransformT> transform) {
      // TODO(ibzib) make this work for upstream transforms that aren't sources.
      // i.e. - instead of PTransform<PBegin, V>, make K generic.
      return PTransformReplacement.of(
          transform.getPipeline().begin(),
          ((ProjectionProducer<PTransform<PBegin, PCollection<Row>>>) transform.getTransform())
              .actuateProjectionPushdown(fields));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<Row> newOutput) {
      // TODO(ibzib) handle mutlipel outputs.
      // i.e. - instead of PTransform<K, PCollection<Row>>, make V generic.
      Map.Entry<TupleTag<?>, PCollection<?>> original =
          Iterables.getOnlyElement(outputs.entrySet());
      Map.Entry<TupleTag<?>, PCollection<?>> replacement =
          (Map.Entry) Iterables.getOnlyElement(newOutput.expand().entrySet());
      return Collections.singletonMap(
          newOutput,
          ReplacementOutput.of(
              TaggedPValue.of(original.getKey(), original.getValue()),
              TaggedPValue.of(replacement.getKey(), replacement.getValue())));
    }
  }
}
