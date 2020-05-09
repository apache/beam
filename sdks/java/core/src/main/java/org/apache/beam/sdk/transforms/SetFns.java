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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
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
   * Returns a new {@code PTransform} transform that follows SET DISTINCT semantics to compute the
   * intersection with provided {@code PCollection<T>}.
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
   *     left.apply(SetFns.intersectDistinct(right)); // results will be PCollection<String> containing: "1","3","4"
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> SetImpl<T> intersectDistinct(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, intersectDistinct());
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollectionList<PCollection<T>>} and returns a
   * {@code PCollection<T>} containing intersection of collections done in order for all collections
   * in {@code PCollectionList<T>}.
   *
   * <p>Returns a new {@code PTransform} transform that follows SET DISTINCT semantics which takes a
   * {@code PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing
   * intersection of collections done in order for all collections in {@code PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} will have all distinct elements that are
   * present in both pipeline is constructed and next {@link PCollection} in the list and applied to
   * all collections in order.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of first in {@code PCollectionList<T>}
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
   *     .apply(SetFns.intersectDistinct()); // results will be PCollection<String> containing: "3","4"
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input {@code PCollectionList<T>} and output {@code
   *     PCollection<T>}s.
   */
  public static <T> SetImplCollections<T> intersectDistinct() {
    SerializableBiFunction<Long, Long, Long> intersectFn =
        (numberOfElementsinLeft, numberOfElementsinRight) ->
            (numberOfElementsinLeft > 0 && numberOfElementsinRight > 0) ? 1L : 0L;
    return new SetImplCollections<>(intersectFn);
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET ALL semantics to compute the
   * intersection with provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} which will follow INTESECT_ALL Semantics as
   * follows: Given there are m elements on pipeline which is constructed {@link PCollection} (left)
   * and n elements on in provided {@link PCollection} (right): - it will output MIN(m - n, 0)
   * elements of left for all elements which are present in both left and right.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of {@code PCollection<T>}
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
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> SetImpl<T> intersectAll(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, intersectAll());
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET ALL semantics which takes a {@code
   * PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing intersection
   * all of collections done in order for all collections in {@code PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} which will follow INTERSECT_ALL semantics.
   * Output is calculated as follows: Given there are m elements on pipeline which is constructed
   * {@link PCollection} (left) and n elements on in provided {@link PCollection} (right): - it will
   * output MIN(m - n, 0) elements of left for all elements which are present in both left and
   * right.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of first in {@code PCollectionList<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   * PCollection<String> first = p.apply(Create.of("1","1","1", "2", "3", "3","4", "5"));
   * PCollection<String> second = p.apply(Create.of("1","1", "3", "4","4", "6"));
   * PCollection<String> third = p.apply(Create.of("1", "5"));
   *
   * // Following example will perform (first intersect second) intersect third.
   * PCollection<String> results =
   *     PCollectionList.of(first).and(second).and(third)
   *     .apply(SetFns.intersectAll()); // results will be PCollection<String> containing: "1","2","3","3"
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input {@code PCollectionList<T>} and output {@code
   *     PCollection<T>}s.
   */
  public static <T> SetImplCollections<T> intersectAll() {
    return new SetImplCollections<>(Math::min);
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET DISTINCT semantics to compute the
   * difference (except) with provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} will all distinct elements that present in
   * pipeline is constructed but not present in provided {@link PCollection}.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of {@code PCollection<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1","1","1", "2", "3", "3","4", "5"));
   * PCollection<String> right = p.apply(Create.of("1","1", "3", "4","4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.exceptDistinct(right)); // results will be PCollection<String> containing: "2","5"
   * }</pre>
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> SetImpl<T> exceptDistinct(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, exceptDistinct());
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollectionList<PCollection<T>>} and returns a
   * {@code PCollection<T>} containing difference (except) of collections done in order for all
   * collections in {@code PCollectionList<T>}.
   *
   * <p>Returns a new {@code PTransform} transform that follows SET DISTINCT semantics which takes a
   * {@code PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing
   * difference (except) of collections done in order for all collections in {@code
   * PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} will have all distinct elements that are
   * present in pipeline is constructed but not present in next {@link PCollection} in the list and
   * applied to all collections in order.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of first in {@code PCollectionList<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   * PCollection<String> first = p.apply(Create.of("1","1","1", "2", "3", "3","4", "5"));
   * PCollection<String> second = p.apply(Create.of("1","1", "3", "4","4", "6"));
   *
   * PCollection<String> third = p.apply(Create.of("1", "2","2"));
   *
   * // Following example will perform (first intersect second) intersect third.
   * PCollection<String> results =
   *     PCollectionList.of(first).and(second).and(third)
   *     .apply(SetFns.exceptDistinct()); // results will be PCollection<String> containing: "5"
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input {@code PCollectionList<T>} and output {@code
   *     PCollection<T>}s.
   */
  public static <T> SetImplCollections<T> exceptDistinct() {
    SerializableBiFunction<Long, Long, Long> exceptFn =
        (numberOfElementsinLeft, numberOfElementsinRight) ->
            numberOfElementsinLeft > 0 && numberOfElementsinRight == 0 ? 1L : 0L;
    return new SetImplCollections<>(exceptFn);
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET ALL semantics to compute the
   * difference all (exceptAll) with provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} which will follow EXCEPT_ALL Semantics as
   * follows: Given there are m elements on pipeline which is constructed {@link PCollection} (left)
   * and n elements on in provided {@link PCollection} (right): - it will output m elements of left
   * for all elements which are present in left but not in right. - it will output MAX(m - n, 0)
   * elements of left for all elements which are present in both left and right.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of {@code PCollection<T>}
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
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> SetImpl<T> exceptAll(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, exceptAll());
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET ALL semantics which takes a {@code
   * PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing difference all
   * (exceptAll) of collections done in order for all collections in {@code PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} which will follow EXCEPT_ALL semantics.
   * Output is calculated as follows: Given there are m elements on pipeline which is constructed
   * {@link PCollection} (left) and n elements on in provided {@link PCollection} (right): - it will
   * output m elements of left for all elements which are present in left but not in right. - it
   * will output MAX(m - n, 0) elements of left for all elements which are present in both left and
   * right.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of first in {@code PCollectionList<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   * PCollection<String> first = p.apply(Create.of("1","1","1", "2", "3", "3","3","4", "5"));
   * PCollection<String> second = p.apply(Create.of("1", "3", "4","4", "6"));
   * PCollection<String> third = p.apply(Create.of("1", "5"));
   *
   * // Following example will perform (first intersect second) intersect third.
   * PCollection<String> results =
   *     PCollectionList.of(first).and(second).and(third)
   *     .apply(SetFns.exceptAll()); // results will be PCollection<String> containing: "1","2","3","3","5"
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input {@code PCollectionList<T>} and output {@code
   *     PCollection<T>}s.
   */
  public static <T> SetImplCollections<T> exceptAll() {
    SerializableBiFunction<Long, Long, Long> exceptFn =
        (numberOfElementsinLeft, numberOfElementsinRight) ->
            Math.max(numberOfElementsinLeft - numberOfElementsinRight, 0L);
    return new SetImplCollections<>(exceptFn);
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET DISTINCT semantics to compute the
   * union with provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} will all distinct elements that present in
   * pipeline is constructed or present in provided {@link PCollection}.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of {@code PCollection<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1","1","2"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4","4"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.unionDistinct(right)); // results will be PCollection<String> containing: "1","2","3","4"
   * }</pre>
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> SetImpl<T> unionDistinct(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, unionDistinct());
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET DISTINCT semantics which takes a
   * {@code PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing union
   * of collections done in order for all collections in {@code PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} will have all distinct elements that are
   * present in pipeline is constructed or present in next {@link PCollection} in the list and
   * applied to all collections in order.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of first in {@code PCollectionList<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   * PCollection<String> first = p.apply(Create.of("1","1","2"));
   * PCollection<String> second = p.apply(Create.of("1", "3", "4","4"));
   *
   * PCollection<String> third = p.apply(Create.of("1", "5"));
   *
   * // Following example will perform (first intersect second) intersect third.
   * PCollection<String> results =
   *     PCollectionList.of(first).and(second).and(third)
   *     .apply(SetFns.unionDistinct()); // results will be PCollection<String> containing: "1","2","3","4","5"
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input {@code PCollectionList<T>} and output {@code
   *     PCollection<T>}s.
   */
  public static <T> SetImplCollections<T> unionDistinct() {
    SerializableBiFunction<Long, Long, Long> unionFn =
        (numberOfElementsinLeft, numberOfElementsinRight) -> 1L;
    return new SetImplCollections<>(unionFn);
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET ALL semantics to compute the
   * unionAll with provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} which will follow UNION_ALL semantics as
   * follows: Given there are m elements on pipeline which is constructed {@link PCollection} (left)
   * and n elements on in provided {@link PCollection} (right): - it will output m elements of left
   * and m elements of right.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of {@code PCollection<T>}
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
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> SetImpl<T> unionAll(PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, unionAll());
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET ALL semantics which takes a {@code
   * PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing unionAll of
   * collections done in order for all collections in {@code PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} which will follow UNION_ALL semantics. Output
   * is calculated as follows: Given there are m elements on pipeline which is constructed {@link
   * PCollection} (left) and n elements on in provided {@link PCollection} (right): - it will output
   * m elements of left and m elements of right.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} that of first in {@code PCollectionList<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   * PCollection<String> first = p.apply(Create.of("1","1","2"));
   * PCollection<String> second = p.apply(Create.of("1", "3", "4","4"));
   * PCollection<String> third = p.apply(Create.of("1", "5"));
   *
   * // Following example will perform (first intersect second) intersect third.
   * PCollection<String> results =
   *     PCollectionList.of(first).and(second).and(third)
   *     .apply(SetFns.unionAll()); // results will be PCollection<String> containing: "1","1","1","1","2","3","4","4","5"
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input {@code PCollectionList<T>} and output {@code
   *     PCollection<T>}s.
   */
  public static <T> Flatten.PCollections<T> unionAll() {
    return Flatten.pCollections();
  }

  private static class SetImpl<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private final transient PCollection<T> rightCollection;
    private final PTransform<PCollectionList<T>, PCollection<T>> listTransformFn;

    private SetImpl(
        PCollection<T> rightCollection,
        PTransform<PCollectionList<T>, PCollection<T>> listTransformFn) {
      this.rightCollection = rightCollection;
      this.listTransformFn = listTransformFn;
    }

    @Override
    public PCollection<T> expand(PCollection<T> leftCollection) {
      return PCollectionList.of(leftCollection).and(rightCollection).apply(listTransformFn);
    }
  }

  private static class SetImplCollections<T>
      extends PTransform<PCollectionList<T>, PCollection<T>> {

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
      }
      MapElements<T, KV<T, Void>> elementToVoid =
          MapElements.via(
              new SimpleFunction<T, KV<T, Void>>() {
                @Override
                public KV<T, Void> apply(T element) {
                  return KV.of(element, null);
                }
              });

      PCollection<T> first = all.get(0);
      Pipeline pipeline = first.getPipeline();
      String firstName = first.getName();

      List<TupleTag<Void>> allTags = new ArrayList<>();
      KeyedPCollectionTuple<T> keyedPCollectionTuple = KeyedPCollectionTuple.empty(pipeline);

      for (PCollection<T> col : all) {
        TupleTag<Void> tag = new TupleTag<>();

        PCollection<KV<T, Void>> kvOfElementAndVoid =
            col.apply("PrepareKVs" + col.getName(), elementToVoid);

        allTags.add(tag);
        keyedPCollectionTuple = keyedPCollectionTuple.and(tag, kvOfElementAndVoid);
      }

      PCollection<KV<T, CoGbkResult>> coGbkResults =
          keyedPCollectionTuple.apply("CBKAll" + firstName, CoGroupByKey.create());

      // TODO: lift combiners through the CoGBK.
      PCollection<T> results =
          coGbkResults.apply(
              "FilterSetElement" + firstName,
              ParDo.of(
                  new DoFn<KV<T, CoGbkResult>, T>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      KV<T, CoGbkResult> elementGroups = c.element();
                      CoGbkResult value = elementGroups.getValue();
                      T element = elementGroups.getKey();

                      long numberOfOutputs = Iterables.size(value.getAll(allTags.get(0)));
                      List<TupleTag<Void>> tail = allTags.subList(1, allTags.size());

                      for (TupleTag<Void> tag : tail) {
                        long nextSize = Iterables.size(value.getAll(tag));
                        numberOfOutputs = fn.apply(numberOfOutputs, nextSize);
                      }
                      for (long i = 0L; i < numberOfOutputs; i++) {
                        c.output(element);
                      }
                    }
                  }));

      return results.setCoder(first.getCoder());
    }

    @Override
    public PCollection<T> expand(PCollectionList<T> input) {
      return performSetOperationCollectionList(input, fn);
    }
  }
}
