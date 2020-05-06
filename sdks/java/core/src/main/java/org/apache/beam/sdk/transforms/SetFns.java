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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

public class SetFns {

  /**
   * Returns a new {@code SetFns.SetImpl<T>} transform that compute the intersection with provided
   * {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection<T>} will all distinct elements that present in
   * both pipeline is constructed and provided {@link PCollection<T>}.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "2", "3", "4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.intersect(right));
   * }</pre>
   */
  public static <T> SetImpl<T> intersect(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    SerializableBiFunction<Long, Long, Long> intersectFn =
        (numberOfElementsinLeft, numberOfElementsinRight) ->
            (numberOfElementsinLeft > 0 && numberOfElementsinRight > 0) ? 1L : 0L;
    return new SetImpl<>(rightCollection, intersectFn);
  }

  /**
   * Returns a new {@code SetFns.SetImpl<T>} transform that compute the intersection all with
   * provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection<T>} which will follow EXCEPT_ALL Semantics as
   * follows: Given there are m elements on pipeline which is constructed {@link PCollection<T>}
   * (left) and n elements on in provided {@link PCollection<T>} (right): - it will output MIN(m -
   * n, 0) elements of left for all elements which are present in both left and right.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "2", "3", "4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.intersectAll(right));
   * }</pre>
   */
  public static <T> SetImpl<T> intersectAll(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    SerializableBiFunction<Long, Long, Long> intersectFn =
        (numberOfElementsinLeft, numberOfElementsinRight) ->
            (numberOfElementsinLeft > 0 && numberOfElementsinRight > 0)
                ? Math.min(numberOfElementsinLeft, numberOfElementsinRight)
                : 0L;
    return new SetImpl<>(rightCollection, intersectFn);
  }

  /**
   * Returns a new {@code SetFns.SetImpl<T>} transform that compute the difference (except) with
   * provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection<T>} will all distinct elements that present in
   * pipeline is constructed {@link PCollection<T>} but not present in provided {@link
   * PCollection<T>}.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "2", "3", "4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.except(right));
   * }</pre>
   */
  public static <T> SetImpl<T> except(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    SerializableBiFunction<Long, Long, Long> exceptFn =
        (numberOfElementsinLeft, numberOfElementsinRight) ->
            numberOfElementsinLeft > 0 && numberOfElementsinRight == 0 ? 1L : 0L;
    return new SetImpl<>(rightCollection, exceptFn);
  }

  /**
   * Returns a new {@code SetFns.SetImpl<T>} transform that compute the difference all (exceptAll)
   * with provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection<T>} which will follow EXCEPT_ALL Semantics as
   * follows: Given there are m elements on pipeline which is constructed {@link PCollection<T>}
   * (left) and n elements on in provided {@link PCollection<T>} (right): - it will output m
   * elements of left for all elements which are present in left but not in right. - it will output
   * MAX(m - n, 0) elements of left for all elements which are present in both left and right.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "2", "3", "4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.exceptAll(right));
   * }</pre>
   */
  public static <T> SetImpl<T> exceptAll(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    SerializableBiFunction<Long, Long, Long> exceptFn =
        (numberOfElementsinLeft, numberOfElementsinRight) -> {
          if (numberOfElementsinLeft > 0 && numberOfElementsinRight == 0) {
            return numberOfElementsinLeft;
          } else if (numberOfElementsinLeft > 0 && numberOfElementsinRight > 0) {
            return Math.max(numberOfElementsinLeft - numberOfElementsinRight, 0L);
          }
          return 0L;
        };
    return new SetImpl<>(rightCollection, exceptFn);
  }

  /**
   * Returns a new {@code SetFns.SetImpl<T>} transform that compute the union with provided {@code
   * PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection<T>} will all distinct elements that present in
   * pipeline is constructed {@link PCollection<T>} and {@link PCollection<T>}.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "2", "3", "4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.union(right));
   * }</pre>
   */
  public static <T> SetImpl<T> union(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    SerializableBiFunction<Long, Long, Long> unionFn =
        (numberOfElementsinLeft, numberOfElementsinRight) -> 1L;
    return new SetImpl<>(rightCollection, unionFn);
  }

  /**
   * Returns a new {@code SetFns.SetImpl<T>} transform that compute the unionAll with provided
   * {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection<T>} which will follow UNION_ALL semantics as
   * follows: Given there are m elements on pipeline which is constructed {@link PCollection<T>}
   * (left) and n elements on in provided {@link PCollection<T>} (right): - it will output m
   * elements of left and m elements of right.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "2", "3", "4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.unionAll(right));
   * }</pre>
   */
  public static <T> SetImpl<T> unionAll(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    SerializableBiFunction<Long, Long, Long> unionFn = Long::sum;
    return new SetImpl<>(rightCollection, unionFn);
  }

  private static <T> PCollection<T> performSetOperation(
      PCollection<T> leftCollection,
      PCollection<T> rightCollection,
      SerializableBiFunction<Long, Long, Long> fn) {

    TupleTag<Void> leftCollectionTag = new TupleTag<>();
    TupleTag<Void> rightCollectionTag = new TupleTag<>();

    MapElements<T, KV<T, Void>> elementToVoid =
        MapElements.via(
            new SimpleFunction<T, KV<T, Void>>() {
              @Override
              public KV<T, Void> apply(T element) {
                return KV.of(element, null);
              }
            });

    PCollection<KV<T, Void>> left = leftCollection.apply("PrepareLeftKV", elementToVoid);
    PCollection<KV<T, Void>> right = rightCollection.apply("PrepareRightKV", elementToVoid);

    PCollection<KV<T, CoGbkResult>> coGbkResults =
        KeyedPCollectionTuple.of(leftCollectionTag, left)
            .and(rightCollectionTag, right)
            .apply(CoGroupByKey.create());

    return coGbkResults.apply(
        ParDo.of(
            new DoFn<KV<T, CoGbkResult>, T>() {

              @ProcessElement
              public void processElement(ProcessContext c) {
                KV<T, CoGbkResult> elementGroups = c.element();

                CoGbkResult value = elementGroups.getValue();
                long inFirstSize = Iterables.size(value.getAll(leftCollectionTag));
                long inSecondSize = Iterables.size(value.getAll(rightCollectionTag));

                T element = elementGroups.getKey();
                for (long i = 0L; i < fn.apply(inFirstSize, inSecondSize); i++) {
                  c.output(element);
                }
              }
            }));
  }

  public static class SetImpl<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private final PCollection<T> rightCollection;
    private final SerializableBiFunction<Long, Long, Long> fn;

    private SetImpl(PCollection<T> rightCollection, SerializableBiFunction<Long, Long, Long> fn) {
      this.rightCollection = rightCollection;
      this.fn = fn;
    }

    @Override
    public PCollection<T> expand(PCollection<T> leftCollection) {
      return performSetOperation(leftCollection, rightCollection, fn)
          .setCoder(leftCollection.getCoder());
    }
  }
}
