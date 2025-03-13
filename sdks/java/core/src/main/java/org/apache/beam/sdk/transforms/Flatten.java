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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableLikeCoder;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * {@code Flatten<T>} takes multiple {@code PCollection<T>}s bundled into a {@code
 * PCollectionList<T>} and returns a single {@code PCollection<T>} containing all the elements in
 * all the input {@code PCollection}s. The name "Flatten" suggests taking a list of lists and
 * flattening them into a single list.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<String> pc1 = ...;
 * PCollection<String> pc2 = ...;
 * PCollection<String> pc3 = ...;
 * PCollectionList<String> pcs = PCollectionList.of(pc1).and(pc2).and(pc3);
 * PCollection<String> merged = pcs.apply(Flatten.<String>pCollections());
 * }</pre>
 *
 * <p>By default, the {@code Coder} of the output {@code PCollection} is the same as the {@code
 * Coder} of the first {@code PCollection} in the input {@code PCollectionList} (if the {@code
 * PCollectionList} is non-empty).
 */
public class Flatten {

  /**
   * Returns a {@link PTransform} that flattens a {@link PCollectionList} into a {@link PCollection}
   * containing all the elements of all the {@link PCollection}s in its input.
   *
   * <p>All inputs must have equal {@link WindowFn}s. The output elements of {@code Flatten<T>} are
   * in the same windows and have the same timestamps as their corresponding input elements. The
   * output {@code PCollection} will have the same {@link WindowFn} as all of the inputs.
   *
   * @param <T> the type of the elements in the input and output {@code PCollection}s.
   */
  public static <T> PCollections<T> pCollections() {
    return new PCollections<>();
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<Iterable<T>>} and returns a {@code
   * PCollection<T>} containing all the elements from all the {@code Iterable}s.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Iterable<Integer>> pcOfIterables = ...;
   * PCollection<Integer> pc = pcOfIterables.apply(Flatten.<Integer>iterables());
   * }</pre>
   *
   * <p>By default, the output {@code PCollection} encodes its elements using the same {@code Coder}
   * that the input uses for the elements in its {@code Iterable}.
   *
   * @param <T> the type of the elements of the input {@code Iterable} and the output {@code
   *     PCollection}
   */
  public static <T> Iterables<T> iterables() {
    return new Iterables<>();
  }

  /**
   * Returns a {@link PTransform} that flattens the input {@link PCollection} with a given {@link
   * PCollection} resulting in a {@link PCollection} containing all the elements of both {@link
   * PCollection}s as its output.
   *
   * <p>This is equivalent to creating a {@link PCollectionList} containing both the input and
   * {@code other} and then applying {@link #pCollections()}, but has the advantage that it can be
   * more easily used inline.
   *
   * <p>Both {@cpde PCollections} must have equal {@link WindowFn}s. The output elements of {@code
   * Flatten<T>} are in the same windows and have the same timestamps as their corresponding input
   * elements. The output {@code PCollection} will have the same {@link WindowFn} as both inputs.
   *
   * @param other the other PCollection to flatten with the input
   * @param <T> the type of the elements in the input and output {@code PCollection}s.
   */
  public static <T> PTransform<PCollection<T>, PCollection<T>> with(PCollection<T> other) {
    return new FlattenWithPCollection<>(other);
  }

  /** Implementation of {@link #with(PCollection)}. */
  private static class FlattenWithPCollection<T>
      extends PTransform<PCollection<T>, PCollection<T>> {
    // We only need to access this at pipeline construction time.
    private final transient PCollection<T> other;

    public FlattenWithPCollection(PCollection<T> other) {
      this.other = other;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return PCollectionList.of(input).and(other).apply(pCollections());
    }

    @Override
    public String getKindString() {
      return "Flatten.With";
    }
  }

  /**
   * Returns a {@link PTransform} that flattens the input {@link PCollection} with the output of
   * another {@link PTransform} resulting in a {@link PCollection} containing all the elements of
   * both the input {@link PCollection}s and the output of the given {@link PTransform} as its
   * output.
   *
   * <p>This is equivalent to creating a {@link PCollectionList} containing both the input and the
   * output of {@code other} and then applying {@link #pCollections()}, but has the advantage that
   * it can be more easily used inline.
   *
   * <p>Both {@code PCollections} must have equal {@link WindowFn}s. The output elements of {@code
   * Flatten<T>} are in the same windows and have the same timestamps as their corresponding input
   * elements. The output {@code PCollection} will have the same {@link WindowFn} as both inputs.
   *
   * @param <T> the type of the elements in the input and output {@code PCollection}s.
   * @param other a PTransform whose ouptput should be flattened with the input
   */
  public static <T> PTransform<PCollection<T>, PCollection<T>> with(
      PTransform<PBegin, PCollection<T>> other) {
    return new PTransform<PCollection<T>, PCollection<T>>() {
      @Override
      public PCollection<T> expand(PCollection<T> input) {
        return PCollectionList.of(input)
            .and(input.getPipeline().apply(other))
            .apply(pCollections());
      }

      @Override
      public String getKindString() {
        return "Flatten.With";
      }
    };
  }

  /**
   * A {@link PTransform} that flattens a {@link PCollectionList} into a {@link PCollection}
   * containing all the elements of all the {@link PCollection}s in its input. Implements {@link
   * #pCollections}.
   *
   * @param <T> the type of the elements in the input and output {@code PCollection}s.
   */
  public static class PCollections<T> extends PTransform<PCollectionList<T>, PCollection<T>> {

    private PCollections() {}

    @Override
    public PCollection<T> expand(PCollectionList<T> inputs) {
      WindowingStrategy<?, ?> windowingStrategy;
      IsBounded isBounded = IsBounded.BOUNDED;
      if (!inputs.getAll().isEmpty()) {
        windowingStrategy = inputs.get(0).getWindowingStrategy();
        for (PCollection<?> input : inputs.getAll()) {
          WindowingStrategy<?, ?> other = input.getWindowingStrategy();
          if (!windowingStrategy.getWindowFn().isCompatible(other.getWindowFn())) {
            throw new IllegalStateException(
                "Inputs to Flatten had incompatible window windowFns: "
                    + windowingStrategy.getWindowFn()
                    + ", "
                    + other.getWindowFn());
          }

          if (!windowingStrategy.getTrigger().isCompatible(other.getTrigger())) {
            throw new IllegalStateException(
                "Inputs to Flatten had incompatible triggers: "
                    + windowingStrategy.getTrigger()
                    + ", "
                    + other.getTrigger());
          }
          isBounded = isBounded.and(input.isBounded());
        }
      } else {
        windowingStrategy = WindowingStrategy.globalDefault();
      }

      return PCollection.createPrimitiveOutputInternal(
          inputs.getPipeline(),
          windowingStrategy,
          isBounded,
          // Take coder from first collection. If there are none, will be left unspecified.
          inputs.getAll().isEmpty() ? null : inputs.get(0).getCoder());
    }
  }

  /**
   * {@code FlattenIterables<T>} takes a {@code PCollection<Iterable<T>>} and returns a {@code
   * PCollection<T>} that contains all the elements from each iterable. Implements {@link
   * #iterables}.
   *
   * @param <T> the type of the elements of the input {@code Iterable}s and the output {@code
   *     PCollection}
   */
  public static class Iterables<T>
      extends PTransform<PCollection<? extends Iterable<T>>, PCollection<T>> {
    private Iterables() {}

    @Override
    public PCollection<T> expand(PCollection<? extends Iterable<T>> in) {
      Coder<? extends Iterable<T>> inCoder = in.getCoder();
      if (!(inCoder instanceof IterableLikeCoder)) {
        throw new IllegalArgumentException(
            "expecting the input Coder<Iterable> to be an IterableLikeCoder");
      }
      @SuppressWarnings("unchecked")
      Coder<T> elemCoder = ((IterableLikeCoder<T, ?>) inCoder).getElemCoder();

      return in.apply(
              "FlattenIterables",
              FlatMapElements.via(
                  new SimpleFunction<Iterable<T>, Iterable<T>>() {
                    @Override
                    public Iterable<T> apply(Iterable<T> element) {
                      return element;
                    }
                  }))
          .setCoder(elemCoder);
    }
  }
}
