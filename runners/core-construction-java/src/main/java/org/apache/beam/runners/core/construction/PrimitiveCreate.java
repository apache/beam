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

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * An implementation of {@link Create} that returns a primitive {@link PCollection}.
 */
public class PrimitiveCreate<T> extends PTransform<PBegin, PCollection<T>> {
  private final Create.Values<T> transform;

  private PrimitiveCreate(Create.Values<T> transform) {
    this.transform = transform;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.BOUNDED);
  }

  public Iterable<T> getElements() {
    return transform.getElements();
  }

  /**
   * A {@link PTransformOverrideFactory} that creates instances of {@link PrimitiveCreate}.
   */
  public static class Factory<T>
      implements PTransformOverrideFactory<PBegin, PCollection<T>, Values<T>> {
    @Override
    public PTransform<PBegin, PCollection<T>> getReplacementTransform(Values<T> transform) {
      return new PrimitiveCreate<>(transform);
    }

    @Override
    public PBegin getInput(List<TaggedPValue> inputs, Pipeline p) {
      return p.begin();
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        List<TaggedPValue> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }
}

