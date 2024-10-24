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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.ProjectionProducer;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** See {@link #optimize(Pipeline)}. */
public class ProjectionPushdownOptimizer {
  private static final Logger LOG = LoggerFactory.getLogger(ProjectionPushdownOptimizer.class);

  /**
   * Performs all known projection pushdown optimizations in-place on a Pipeline.
   *
   * <p>A pushdown optimization is possible wherever there is a {@link ProjectionProducer} that
   * produces a {@link PCollection} that is consumed by one or more PTransforms with an annotated
   * {@link FieldAccessDescriptor}, where the number of fields consumed is less than the number of
   * fields produced. The optimizer replaces the {@link ProjectionProducer} with the result of
   * calling {@link ProjectionProducer#actuateProjectionPushdown(Map)} on that producer with those
   * PCollections/fields.
   *
   * <p>Currently only supports pushdown on {@link ProjectionProducer} instances that are applied
   * directly to {@link PBegin} (https://github.com/apache/beam/issues/21359).
   */
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
      for (Entry<TupleTag<?>, FieldAccessDescriptor> outputFields : entry.getValue().entrySet()) {
        LOG.info(
            "Optimizing transform {}: output {} will contain reduced field set {}",
            entry.getKey(),
            outputFields.getKey(),
            outputFields.getValue().fieldNamesAccessed());
      }
      PTransformMatcher matcher = application -> application.getTransform() == entry.getKey();
      PushdownOverrideFactory<?, ?> overrideFactory =
          new PushdownOverrideFactory<>(entry.getValue());
      pipeline.replaceAll(ImmutableList.of(PTransformOverride.of(matcher, overrideFactory)));
    }
  }

  // TODO(https://github.com/apache/beam/issues/21359) Support inputs other than PBegin.
  private static class PushdownOverrideFactory<
          OutputT extends POutput, TransformT extends PTransform<PBegin, OutputT>>
      implements PTransformOverrideFactory<PBegin, OutputT, TransformT> {
    private final Map<TupleTag<?>, FieldAccessDescriptor> fields;

    PushdownOverrideFactory(Map<TupleTag<?>, FieldAccessDescriptor> fields) {
      this.fields = fields;
    }

    @Override
    public PTransformReplacement<PBegin, OutputT> getReplacementTransform(
        AppliedPTransform<PBegin, OutputT, TransformT> transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(),
          ((ProjectionProducer<PTransform<PBegin, OutputT>>) transform.getTransform())
              .actuateProjectionPushdown(fields));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, OutputT newOutput) {
      return outputs.entrySet().stream()
          .map(
              oldOutput -> {
                PCollection<?> newOutputPColl;
                if (newOutput.expand().size() == 1) {
                  // If output is a single PCollection, use it directly.
                  newOutputPColl =
                      (PCollection<?>) Iterables.getOnlyElement(newOutput.expand().values());
                } else {
                  // Output is a PCollectionTuple, look up component PCollections using the original
                  // output tags.
                  newOutputPColl =
                      Preconditions.checkArgumentNotNull(
                          (PCollection<?>) newOutput.expand().get(oldOutput.getKey()),
                          "No PCollection found for output tag %s. Were output tags changed in actuateProjectionPushdown?",
                          oldOutput.getKey());
                }
                return new SimpleEntry<>(
                    newOutputPColl,
                    ReplacementOutput.of(
                        TaggedPValue.ofExpandedValue(oldOutput.getValue()),
                        TaggedPValue.ofExpandedValue(newOutputPColl)));
              })
          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }
  }
}
