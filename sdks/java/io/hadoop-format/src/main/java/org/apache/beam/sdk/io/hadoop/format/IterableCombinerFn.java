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
package org.apache.beam.sdk.io.hadoop.format;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Collects all items of defined type into one {@link Iterable} container.
 *
 * @param <T> Type of the elements to collect
 */
class IterableCombinerFn<T>
    extends Combine.AccumulatingCombineFn<
        T, IterableCombinerFn.CollectionAccumulator<T>, Iterable<T>> {

  /**
   * Accumulator for collecting one "shard" of types.
   *
   * @param <T> Type of the elements to collect
   */
  public static class CollectionAccumulator<T>
      implements Combine.AccumulatingCombineFn.Accumulator<
          T, CollectionAccumulator<T>, Iterable<T>> {

    private final List<T> collection;

    private CollectionAccumulator() {
      this(new ArrayList<>());
    }

    private CollectionAccumulator(List<T> collection) {
      Objects.requireNonNull(collection, "Collection can't be null");
      this.collection = collection;
    }

    @Override
    public void addInput(T input) {
      collection.add(input);
    }

    @Override
    public void mergeAccumulator(CollectionAccumulator<T> other) {
      collection.addAll(other.collection);
    }

    @Override
    public Iterable<T> extractOutput() {
      return collection;
    }
  }

  private final TypeDescriptor<T> typeDescriptor;

  IterableCombinerFn(TypeDescriptor<T> typeDescriptor) {
    this.typeDescriptor = typeDescriptor;
  }

  @Override
  public CollectionAccumulator<T> createAccumulator() {
    return new CollectionAccumulator<>();
  }

  @Override
  public TypeDescriptor<Iterable<T>> getOutputType() {
    return TypeDescriptors.iterables(typeDescriptor);
  }

  @Override
  public Coder<Iterable<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
    return IterableCoder.of(inputCoder);
  }

  @Override
  public Coder<CollectionAccumulator<T>> getAccumulatorCoder(
      CoderRegistry registry, Coder<T> inputCoder) {
    return new CollectionAccumulatorCoder<>(inputCoder);
  }

  /**
   * Coder for {@link CollectionAccumulator} class.
   *
   * @param <T> Type of the {@link CollectionAccumulator} class
   */
  private static class CollectionAccumulatorCoder<T> extends AtomicCoder<CollectionAccumulator<T>> {

    /** List coder is used to en/decode {@link CollectionAccumulator}. */
    private final ListCoder<T> listCoder;

    /**
     * Ctor requires coder for the element type.
     *
     * @param typeCoder coder for the element type
     */
    private CollectionAccumulatorCoder(Coder<T> typeCoder) {
      this.listCoder = ListCoder.of(typeCoder);
    }

    @Override
    public void encode(IterableCombinerFn.CollectionAccumulator<T> value, OutputStream outStream)
        throws IOException {
      listCoder.encode(value.collection, outStream);
    }

    @Override
    public IterableCombinerFn.CollectionAccumulator<T> decode(InputStream inStream)
        throws IOException {

      List<T> decodedList = listCoder.decode(inStream);
      return new IterableCombinerFn.CollectionAccumulator<>(decodedList);
    }
  }
}
