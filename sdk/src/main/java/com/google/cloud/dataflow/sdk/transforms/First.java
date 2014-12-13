/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

/**
 * {@code First<T>} takes a {@code PCollection<T>} and a limit, and
 * produces a new {@code PCollection<T>} containing up to limit
 * elements of the input {@code PCollection}.
 *
 * <p> If the input and output {@code PCollection}s are ordered, then
 * {@code First} will select the first elements, otherwise it will
 * select any elements.
 *
 * <p> If limit is less than or equal to the size of the input
 * {@code PCollection}, then all the input's elements will be selected.
 *
 * <p> All of the elements of the output {@code PCollection} should fit into
 * main memory of a single worker machine.  This operation does not
 * run in parallel.
 *
 * <p> Example of use:
 * <pre> {@code
 * PCollection<String> input = ...;
 * PCollection<String> output = input.apply(First.<String>of(100));
 * } </pre>
 *
 * @param <T> the type of the elements of the input and output
 * {@code PCollection}s
 */
public class First<T> extends PTransform<PCollection<T>, PCollection<T>> {
  /**
   * Returns a {@code First<T>} {@code PTransform}.
   *
   * @param <T> the type of the elements of the input and output
   * {@code PCollection}s
   * @param limit the numer of elements to take from the input
   */
  public static <T> First<T> of(long limit) {
    return new First<>(limit);
  }

  private final long limit;

  /**
   * Constructs a {@code First<T>} PTransform that, when applied,
   * produces a new PCollection containing up to {@code limit}
   * elements of its input {@code PCollection}.
   */
  private First(long limit) {
    this.limit = limit;
    if (limit < 0) {
      throw new IllegalArgumentException(
          "limit argument to First should be non-negative");
    }
  }

  private static class CopyFirstDoFn<T> extends DoFn<Void, T> {
    long limit;
    final PCollectionView<Iterable<T>, ?> iterableView;

    public CopyFirstDoFn(long limit, PCollectionView<Iterable<T>, ?> iterableView) {
      this.limit = limit;
      this.iterableView = iterableView;
    }

    @Override
    public void processElement(ProcessContext c) {
      for (T i : c.sideInput(iterableView)) {
        if (limit-- <= 0) {
          break;
        }
        c.output(i);
      }
    }
  }

  @Override
  public PCollection<T> apply(PCollection<T> in) {
    PCollectionView<Iterable<T>, ?> iterableView = in.apply(View.<T>asIterable());
    return
        in.getPipeline()
        .apply(Create.of((Void) null)).setCoder(VoidCoder.of())
        .apply(ParDo
               .withSideInputs(iterableView)
               .of(new CopyFirstDoFn<>(limit, iterableView)))
        .setCoder(in.getCoder());
  }
}
