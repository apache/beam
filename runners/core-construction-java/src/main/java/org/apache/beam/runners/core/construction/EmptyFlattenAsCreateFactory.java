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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Flatten.FlattenPCollectionList;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * A {@link PTransformOverrideFactory} that provides an empty {@link Create} to replace a {@link
 * Flatten.FlattenPCollectionList} that takes no input {@link PCollection PCollections}.
 */
public class EmptyFlattenAsCreateFactory<T>
    implements PTransformOverrideFactory<
        PCollectionList<T>, PCollection<T>, Flatten.FlattenPCollectionList<T>> {
  private static final EmptyFlattenAsCreateFactory<Object> INSTANCE =
      new EmptyFlattenAsCreateFactory<>();

  public static <T> EmptyFlattenAsCreateFactory<T> instance() {
    return (EmptyFlattenAsCreateFactory<T>) INSTANCE;
  }

  private EmptyFlattenAsCreateFactory() {}

  @Override
  public PTransform<PCollectionList<T>, PCollection<T>> getReplacementTransform(
      FlattenPCollectionList<T> transform) {
    return (PTransform) Create.empty(VoidCoder.of());
  }

  @Override
  public PCollectionList<T> getInput(
      List<TaggedPValue> inputs, Pipeline p) {
    checkArgument(
        inputs.isEmpty(), "Must have an empty input to use %s", getClass().getSimpleName());
    return PCollectionList.empty(p);
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      List<TaggedPValue> outputs, PCollection<T> newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }
}
