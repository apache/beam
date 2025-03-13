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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A transform for extracting stamp and applying a user-defined transform on the extracted
 * collection.
 *
 * @param <InputT> input type
 * @param <OutputT> output type
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Deprecated
public class TimestampExtractTransform<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

  /**
   * Create transformation working on KVs with timestamps as key.
   *
   * @param <InputT> type of input elements
   * @param <OutputT> type of output elements
   * @param transform the transform applied on elements with timestamp
   * @return the transform
   */
  public static <InputT, OutputT> TimestampExtractTransform<InputT, OutputT> of(
      PCollectionTransform<InputT, OutputT> transform) {

    return new TimestampExtractTransform<>(null, transform);
  }

  /**
   * Create transformation working on KVs with timestamps as key.
   *
   * @param <InputT> type of input elements
   * @param <OutputT> type of output elements
   * @param name name of the transform
   * @param transform the transform applied on elements with timestamp
   * @return the transform
   */
  public static <InputT, OutputT> TimestampExtractTransform<InputT, OutputT> of(
      String name, PCollectionTransform<InputT, OutputT> transform) {

    return new TimestampExtractTransform<>(name, transform);
  }

  /**
   * Apply user defined transform(s) to input {@link PCollection} and return output one.
   *
   * @param <InputT> input type
   * @param <OutputT> output type
   */
  @FunctionalInterface
  public interface PCollectionTransform<InputT, OutputT>
      extends UnaryFunction<PCollection<KV<Long, InputT>>, PCollection<OutputT>> {}

  private static class Wrap<T> extends DoFn<T, KV<Long, T>> {

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(KV.of(ctx.timestamp().getMillis(), ctx.element()));
    }
  }

  private final PCollectionTransform<InputT, OutputT> timestampedTransform;

  private TimestampExtractTransform(
      @Nullable String name, PCollectionTransform<InputT, OutputT> timestampedTransform) {

    super(name);
    this.timestampedTransform = timestampedTransform;
  }

  @Override
  public PCollection<OutputT> expand(PCollection<InputT> input) {
    PCollection<KV<Long, InputT>> in;
    in = input.apply(getName("wrap"), ParDo.of(new Wrap<>()));
    if (input.getTypeDescriptor() != null) {
      in =
          in.setTypeDescriptor(
              TypeDescriptors.kvs(TypeDescriptors.longs(), input.getTypeDescriptor()));
    }
    return timestampedTransform.apply(in);
  }

  private String getName(String suffix) {
    return name != null ? name + "::" + suffix : suffix;
  }
}
