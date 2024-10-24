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

import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/** An implementation of {@link Create} that returns a primitive {@link PCollection}. */
public class PrimitiveCreate<T> extends PTransform<PBegin, PCollection<T>> {
  private final Create.Values<T> transform;
  private final Coder<T> coder;

  private PrimitiveCreate(Create.Values<T> transform, Coder<T> coder) {
    this.transform = transform;
    this.coder = coder;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.BOUNDED, coder);
  }

  public Iterable<T> getElements() {
    return transform.getElements();
  }

  /** A {@link PTransformOverrideFactory} that creates instances of {@link PrimitiveCreate}. */
  public static class Factory<T>
      implements PTransformOverrideFactory<PBegin, PCollection<T>, Values<T>> {
    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, Values<T>> transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(),
          new PrimitiveCreate<>(
              transform.getTransform(),
              ((PCollection<T>) Iterables.getOnlyElement(transform.getOutputs().values()))
                  .getCoder()));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }
}
