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

package org.apache.beam.runners.core.construction;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Flatten.PCollections;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link PTransformOverrideFactory} that will apply a flatten where no element appears in the
 * input {@link PCollectionList} more than once.
 */
public class DeduplicatedFlattenFactory<T>
    implements PTransformOverrideFactory<
        PCollectionList<T>, PCollection<T>, Flatten.PCollections<T>> {

  public static <T> DeduplicatedFlattenFactory<T> create() {
    return new DeduplicatedFlattenFactory<>();
  }

  private DeduplicatedFlattenFactory() {}

  @Override
  public PTransformReplacement<PCollectionList<T>, PCollection<T>> getReplacementTransform(
      AppliedPTransform<PCollectionList<T>, PCollection<T>, PCollections<T>> transform) {
    return PTransformReplacement.of(
        getInput(transform.getInputs(), transform.getPipeline()),
        new FlattenWithoutDuplicateInputs<T>());
  }

  /**
   * {@inheritDoc}.
   *
   * <p>The input {@link PCollectionList} that is constructed will have the same values in the same
   */
  private PCollectionList<T> getInput(Map<TupleTag<?>, PValue> inputs, Pipeline p) {
    PCollectionList<T> pCollections = PCollectionList.empty(p);
    for (PValue input : inputs.values()) {
      PCollection<T> pcollection = (PCollection<T>) input;
      pCollections = pCollections.and(pcollection);
    }
    return pCollections;
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      Map<TupleTag<?>, PValue> outputs, PCollection<T> newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }

  @VisibleForTesting
  static class FlattenWithoutDuplicateInputs<T>
      extends PTransform<PCollectionList<T>, PCollection<T>> {
    @Override
    public PCollection<T> expand(PCollectionList<T> input) {
      Map<PCollection<T>, Integer> instances = new HashMap<>();
      for (PCollection<T> pCollection : input.getAll()) {
        int existing = instances.get(pCollection) == null ? 0 : instances.get(pCollection);
        instances.put(pCollection, existing + 1);
      }
      PCollectionList<T> output = PCollectionList.empty(input.getPipeline());
      for (Map.Entry<PCollection<T>, Integer> instanceEntry : instances.entrySet()) {
        if (instanceEntry.getValue().equals(1)) {
          output = output.and(instanceEntry.getKey());
        } else {
          String duplicationName = String.format("Multiply %s", instanceEntry.getKey().getName());
          PCollection<T> duplicated =
              instanceEntry
                  .getKey()
                  .apply(duplicationName, ParDo.of(new DuplicateFn<T>(instanceEntry.getValue())));
          output = output.and(duplicated);
        }
      }
      return output.apply(Flatten.<T>pCollections());
    }
  }

  private static class DuplicateFn<T> extends DoFn<T, T> {
    private final int numTimes;

    private DuplicateFn(int numTimes) {
      this.numTimes = numTimes;
    }

    @ProcessElement
    public void emitCopies(ProcessContext context) {
      for (int i = 0; i < numTimes; i++) {
        context.output(context.element());
      }
    }
  }
}
