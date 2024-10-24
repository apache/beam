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

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;

/**
 * Utilities for ensuring that all {@link Read} {@link PTransform PTransforms} are consumed by some
 * {@link PTransform}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class UnconsumedReads {
  public static void ensureAllReadsConsumed(Pipeline pipeline) {
    final Set<PCollection<?>> unconsumed = new HashSet<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void visitPrimitiveTransform(Node node) {
            unconsumed.removeAll(node.getInputs().values());
          }

          @Override
          public void visitValue(PValue value, Node producer) {
            String urn = PTransformTranslation.urnForTransformOrNull(producer.getTransform());
            if (PTransformTranslation.READ_TRANSFORM_URN.equals(urn)) {
              unconsumed.add((PCollection<?>) value);
            }
          }
        });
    int i = 0;
    for (PCollection<?> unconsumedPCollection : unconsumed) {
      consume(unconsumedPCollection, i);
      i++;
    }
  }

  private static <T> void consume(PCollection<T> unconsumedPCollection, int uniq) {
    // Multiple applications should never break due to stable unique names.
    String uniqueName = "DropInputs" + (uniq == 0 ? "" : uniq);
    unconsumedPCollection.apply(uniqueName, ParDo.of(new NoOpDoFn<>()));
  }

  private static class NoOpDoFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void doNothing(ProcessContext context) {}
  }
}
