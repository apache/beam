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

import java.util.List;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

public class SetFns {

  /**
   * Returns a new {@code PTransform} transform that compute the intersection with provided {@code
   * PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} will all distinct elements that present in
   * both pipeline is constructed and provided {@link PCollection}.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of {@code PCollection<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "2", "3", "3","4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4","4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.intersect(right)); // results will be PCollection<String> containing: "1","3","4"
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> SetImpl<T> intersect(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, intersect());
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollectionList<PCollection<T>>} and returns a
   * {@code PCollection<T>} containing intersection of collections done in order for all collections
   * in {@code PCollectionList<T>}.
   *
   * <p>Intersection follows SET DISTINCT semantics.The elements of the output {@link
   * PCollection} will have all distinct elements that present in both pipeline is constructed
   * and next {@link PCollection} in the list and applied to all collections in order.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> first = p.apply(Create.of("1", "2", "3", "3","4", "5"));
   * PCollection<String> second = p.apply(Create.of("1", "3", "4","4", "6"));
   * PCollection<String> third = p.apply(Create.of("3", "4","4"));
   *
   * // Following example will perform (first intersect second) intersect third.
   * PCollection<String> results =
   *     PCollectionList.of(first).and(second).and(third)
   *     .apply(SetFns.intersect()); // results will be PCollection<String> containing: "3","4"
   *
   * }</pre>
   */
  public static <T> SetImplCollections<T> intersect() {
    SerializableBiFunction<Long, Long, Long> intersectFn =
        (numberOfElementsinLeft, numberOfElementsinRight) ->
            (numberOfElementsinLeft > 0 && numberOfElementsinRight > 0) ? 1L : 0L;
    return new SetImplCollections<>(intersectFn);
  }

  /**
   * Returns a new {@code SetFns.SetImpl<T>} transform that compute the intersection all with
   * provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} which will follow INTESECT_ALL Semantics as
   * follows: Given there are m elements on pipeline which is constructed {@link PCollection} (left)
   * and n elements on in provided {@link PCollection} (right): - it will output MIN(m - n, 0)
   * elements of left for all elements which are present in both left and right.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1","1","1", "2", "3", "3","4", "5"));
   * PCollection<String> right = p.apply(Create.of("1","1", "3", "4","4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.intersectAll(right)); // results will be PCollection<String> containing: "1","1","3","4"
   * }</pre>
   */
  public static <T> SetImpl<T> intersectAll(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, intersectAll());
  }

  public static <T> SetImplCollections<T> intersectAll() {
    return new SetImplCollections<>(Math::min);
  }

  /**
   * Returns a new {@code SetFns.SetImpl<T>} transform that compute the difference (except) with
   * provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} will all distinct elements that present in
   * pipeline is constructed {@link PCollection} but not present in provided {@link PCollection}.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1","1","1", "2", "3", "3","4", "5"));
   * PCollection<String> right = p.apply(Create.of("1","1", "3", "4","4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.except(right)); // results will be PCollection<String> containing: "2","5"
   * }</pre>
   */
  public static <T> SetImpl<T> except(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, except());
  }

  public static <T> SetImplCollections<T> except() {
    SerializableBiFunction<Long, Long, Long> exceptFn =
        (numberOfElementsinLeft, numberOfElementsinRight) ->
            numberOfElementsinLeft > 0 && numberOfElementsinRight == 0 ? 1L : 0L;
    return new SetImplCollections<>(exceptFn);
  }

  /**
   * Returns a new {@code SetFns.SetImpl<T>} transform that compute the difference all (exceptAll)
   * with provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} which will follow EXCEPT_ALL Semantics as
   * follows: Given there are m elements on pipeline which is constructed {@link PCollection} (left)
   * and n elements on in provided {@link PCollection} (right): - it will output m elements of left
   * for all elements which are present in left but not in right. - it will output MAX(m - n, 0)
   * elements of left for all elements which are present in both left and right.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1","1","1", "2", "3", "3","3","4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4","4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.exceptAll(right)); // results will be PCollection<String> containing: "1","1","2","3","3","5"
   * }</pre>
   */
  public static <T> SetImpl<T> exceptAll(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, exceptAll());
  }

  public static <T> SetImplCollections<T> exceptAll() {
    SerializableBiFunction<Long, Long, Long> exceptFn =
        (numberOfElementsinLeft, numberOfElementsinRight) ->
            Math.max(numberOfElementsinLeft - numberOfElementsinRight, 0L);
    return new SetImplCollections<>(exceptFn);
  }

  /**
   * Returns a new {@code SetFns.SetImpl<T>} transform that compute the union with provided {@code
   * PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} will all distinct elements that present in
   * pipeline is constructed {@link PCollection} and {@link PCollection}.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1","1","2"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4","4"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.union(right)); // results will be PCollection<String> containing: "1","2","3","4"
   * }</pre>
   */
  public static <T> SetImpl<T> distinctUnion(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, distinctUnion());
  }

  public static <T> SetImplCollections<T> distinctUnion() {
    SerializableBiFunction<Long, Long, Long> unionFn =
        (numberOfElementsinLeft, numberOfElementsinRight) -> 1L;
    return new SetImplCollections<>(unionFn);
  }

  /**
   * Returns a new {@code SetFns.SetUnionAllImpl<T>} transform that compute the unionAll with
   * provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} which will follow UNION_ALL semantics as
   * follows: Given there are m elements on pipeline which is constructed {@link PCollection} (left)
   * and n elements on in provided {@link PCollection} (right): - it will output m elements of left
   * and m elements of right.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1","1","2"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4","4"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.unionAll(right)); // results will be PCollection<String> containing: "1","1","1","2","3","4","4"
   * }</pre>
   */
  public static <T> SetImpl<T> unionAll(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, unionAll());
  }

  public static <T> Flatten.PCollections<T> unionAll() {
    return Flatten.pCollections();
  }

  public static class SetImpl<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private final transient PCollection<T> right;
    private final PTransform<PCollectionList<T>, PCollection<T>> listTransformFn;

    private SetImpl(
        PCollection<T> rightCollection,
        PTransform<PCollectionList<T>, PCollection<T>> listTransformFn) {
      this.right = rightCollection;
      this.listTransformFn = listTransformFn;
    }

    @Override
    public PCollection<T> expand(PCollection<T> leftCollection) {
      return PCollectionList.of(leftCollection).and(right).apply(listTransformFn);
    }
  }

  public static class SetImplCollections<T> extends PTransform<PCollectionList<T>, PCollection<T>> {

    private final transient SerializableBiFunction<Long, Long, Long> fn;

    private SetImplCollections(SerializableBiFunction<Long, Long, Long> fn) {
      this.fn = fn;
    }

    private static <T> PCollection<T> performSetOperationCollectionList(
            PCollectionList<T> inputs, SerializableBiFunction<Long, Long, Long> fn) {
      List<PCollection<T>> all = inputs.getAll();
      int size = all.size();
      if (size == 1) {
        return inputs.get(0); // Handle only one PCollection in list. Coder is already specified
      } else {
        PCollection<T> accum = inputs.get(0);
        List<PCollection<T>> tail = all.subList(1, size);

        for (PCollection<T> second : tail) {
          accum = performSetOperation(accum, second, fn);
        }

        return accum.setCoder(accum.getCoder());
      }
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

      String leftCollectionName = leftCollection.getName();
      String rightCollectionName = rightCollection.getName();
      PCollection<KV<T, Void>> left =
              leftCollection.apply("PrepareLeftKV" + leftCollectionName, elementToVoid);
      PCollection<KV<T, Void>> right =
              rightCollection.apply("PrepareRightKV" + rightCollectionName, elementToVoid);

      PCollection<KV<T, CoGbkResult>> coGbkResults =
              KeyedPCollectionTuple.of(leftCollectionTag, left)
                      .and(rightCollectionTag, right)
                      .apply("CBK" + leftCollectionName, CoGroupByKey.create());
      // TODO: lift combiners through the CoGBK.
      return coGbkResults.apply(
              "FilterSetElement" + leftCollectionName,
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

    @Override
    public PCollection<T> expand(PCollectionList<T> input) {
      return performSetOperationCollectionList(input, fn);
    }
  }
}
