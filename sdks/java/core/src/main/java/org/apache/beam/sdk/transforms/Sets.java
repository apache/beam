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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/**
 * The {@code PTransform}s that allow to compute different set functions across {@link
 * PCollection}s.
 *
 * <p>They come in two variants. 1. Between two {@link PCollection} 2. Between two or more {@link
 * PCollection} in a {@link PCollectionList}.
 *
 * <p>Following {@code PTransform}s follows SET DISTINCT semantics: intersectDistinct,
 * expectDistinct, unionDistinct
 *
 * <p>Following {@code PTransform}s follows SET ALL semantics: intersectAll, expectAll, unionAll
 *
 * <p>For example, the following demonstrates intersectDistinct between two collections {@link
 * PCollection}s.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<String> left = p.apply(Create.of("1", "2", "3", "3", "4", "5"));
 * PCollection<String> right = p.apply(Create.of("1", "3", "4", "4", "6"));
 *
 * PCollection<String> results =
 *     left.apply(SetFns.intersectDistinct(right)); // results will be PCollection<String> containing: "1","3","4"
 *
 * }</pre>
 *
 * <p>For example, the following demonstrates intersectDistinct between three collections {@link
 * PCollection}s in a {@link PCollectionList}.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<String> first = p.apply(Create.of("1", "2", "3", "3", "4", "5"));
 * PCollection<String> second = p.apply(Create.of("1", "3", "4", "4", "6"));
 * PCollection<String> third = p.apply(Create.of("3", "4", "4"));
 *
 * // Following example will perform (first intersect second) intersect third.
 * PCollection<String> results =
 *     PCollectionList.of(first).and(second).and(third)
 *     .apply(SetFns.intersectDistinct()); // results will be PCollection<String> containing: "3","4"
 *
 * }</pre>
 */
public class Sets {

  /**
   * Returns a new {@code PTransform} transform that follows SET DISTINCT semantics to compute the
   * intersection with provided {@code PCollection<T>}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} will all distinct elements that present in
   * both pipeline is constructed and provided {@link PCollection}.
   *
   * <p>Note that this transform requires that the {@code Coder} of the all {@code PCollection<T>}
   * to be deterministic (see {@link Coder#verifyDeterministic()}). If the collection {@code Coder}
   * is not deterministic, an exception is thrown at pipeline construction time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}). Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the input {@code PCollection<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "2", "3", "3", "4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4", "4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.intersectDistinct(right)); // results will be PCollection<String> containing: "1","3","4"
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> PTransform<PCollection<T>, PCollection<T>> intersectDistinct(
      PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, intersectDistinct());
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollectionList<PCollection<T>>} and returns a
   * {@code PCollection<T>} containing the intersection of collections done in order for all
   * collections in {@code PCollectionList<T>}.
   *
   * <p>Returns a new {@code PTransform} transform that follows SET DISTINCT semantics which takes a
   * {@code PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing the
   * intersection of collections done in order for all collections in {@code PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} will have all distinct elements that are
   * present in both pipeline is constructed and next {@link PCollection} in the list and applied to
   * all collections in order.
   *
   * <p>Note that this transform requires that the {@code Coder} of the all {@code PCollection<T>}
   * to be deterministic (see {@link Coder#verifyDeterministic()}). If the collection {@code Coder}
   * is not deterministic, an exception is thrown at pipeline construction time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}).Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the first {@code PCollection<T>} in {@code PCollectionList<T>}.
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> first = p.apply(Create.of("1", "2", "3", "3", "4", "5"));
   * PCollection<String> second = p.apply(Create.of("1", "3", "4", "4", "6"));
   * PCollection<String> third = p.apply(Create.of("3", "4", "4"));
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
  public static <T> PTransform<PCollectionList<T>, PCollection<T>> intersectDistinct() {
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
   * <p>The elements of the output {@link PCollection} which will follow INTERSECT_ALL Semantics as
   * follows: Given there are m elements on pipeline which is constructed {@link PCollection} (left)
   * and n elements on in provided {@link PCollection} (right): - it will output MIN(m - n, 0)
   * elements of left for all elements which are present in both left and right.
   *
   * <p>Note that this transform requires that the {@code Coder} of the all {@code PCollection<T>}
   * to be deterministic (see {@link Coder#verifyDeterministic()}). If the collection {@code Coder}
   * is not deterministic, an exception is thrown at pipeline construction time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}).Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the input {@code PCollection<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "1", "1", "2", "3", "3", "4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "1", "3", "4", "4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.intersectAll(right)); // results will be PCollection<String> containing: "1","1","3","4"
   * }</pre>
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> PTransform<PCollection<T>, PCollection<T>> intersectAll(
      PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, intersectAll());
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET ALL semantics which takes a {@code
   * PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing the
   * intersection all of collections done in order for all collections in {@code
   * PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} which will follow INTERSECT_ALL semantics.
   * Output is calculated as follows: Given there are m elements on pipeline which is constructed
   * {@link PCollection} (left) and n elements on in provided {@link PCollection} (right): - it will
   * output MIN(m - n, 0) elements of left for all elements which are present in both left and
   * right.
   *
   * <p>Note that this transform requires that the {@code Coder} of the all {@code PCollection<T>}
   * to be deterministic (see {@link Coder#verifyDeterministic()}). If the collection {@code Coder}
   * is not deterministic, an exception is thrown at pipeline construction time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}).Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the first {@code PCollection<T>} in {@code PCollectionList<T>}.
   *
   * <pre>{@code
   * Pipeline p = ...;
   * PCollection<String> first = p.apply(Create.of("1", "1", "1", "2", "3", "3", "4", "5"));
   * PCollection<String> second = p.apply(Create.of("1", "1", "3", "4", "4", "6"));
   * PCollection<String> third = p.apply(Create.of("1", "5"));
   *
   * // Following example will perform (first intersect second) intersect third.
   * PCollection<String> results =
   *     PCollectionList.of(first).and(second).and(third)
   *     .apply(SetFns.intersectAll()); // results will be PCollection<String> containing: "1"
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input {@code PCollectionList<T>} and output {@code
   *     PCollection<T>}s.
   */
  public static <T> PTransform<PCollectionList<T>, PCollection<T>> intersectAll() {
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
   * <p>Note that this transform requires that the {@code Coder} of the all {@code PCollection<T>}
   * to be deterministic (see {@link Coder#verifyDeterministic()}). If the collection {@code Coder}
   * is not deterministic, an exception is thrown at pipeline construction time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}).Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the input {@code PCollection<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "1", "1", "2", "3", "3","4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "1", "3", "4", "4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.exceptDistinct(right)); // results will be PCollection<String> containing: "2","5"
   * }</pre>
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> PTransform<PCollection<T>, PCollection<T>> exceptDistinct(
      PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, exceptDistinct());
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollectionList<PCollection<T>>} and returns a
   * {@code PCollection<T>} containing the difference (except) of collections done in order for all
   * collections in {@code PCollectionList<T>}.
   *
   * <p>Returns a new {@code PTransform} transform that follows SET DISTINCT semantics which takes a
   * {@code PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing the
   * difference (except) of collections done in order for all collections in {@code
   * PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} will have all distinct elements that are
   * present in pipeline is constructed but not present in next {@link PCollection} in the list and
   * applied to all collections in order.
   *
   * <p>Note that this transform requires that the {@code Coder} of the all {@code PCollection<T>}
   * to be deterministic (see {@link Coder#verifyDeterministic()}). If the collection {@code Coder}
   * is not deterministic, an exception is thrown at pipeline construction time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}).Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the first {@code PCollection<T>} in {@code PCollectionList<T>}.
   *
   * <pre>{@code
   * Pipeline p = ...;
   * PCollection<String> first = p.apply(Create.of("1", "1", "1", "2", "3", "3", "4", "5"));
   * PCollection<String> second = p.apply(Create.of("1", "1", "3", "4", "4", "6"));
   *
   * PCollection<String> third = p.apply(Create.of("1", "2", "2"));
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
  public static <T> PTransform<PCollectionList<T>, PCollection<T>> exceptDistinct() {
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
   * <p>Note that this transform requires that the {@code Coder} of the all {@code PCollection<T>}
   * to be deterministic (see {@link Coder#verifyDeterministic()}). If the collection {@code Coder}
   * is not deterministic, an exception is thrown at pipeline construction time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}).Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the input {@code PCollection<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "1", "1", "2", "3", "3", "3", "4", "5"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4", "4", "6"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.exceptAll(right)); // results will be PCollection<String> containing: "1","1","2","3","3","5"
   * }</pre>
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> PTransform<PCollection<T>, PCollection<T>> exceptAll(
      PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, exceptAll());
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET ALL semantics which takes a {@code
   * PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing the difference
   * all (exceptAll) of collections done in order for all collections in {@code PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} which will follow EXCEPT_ALL semantics.
   * Output is calculated as follows: Given there are m elements on pipeline which is constructed
   * {@link PCollection} (left) and n elements on in provided {@link PCollection} (right): - it will
   * output m elements of left for all elements which are present in left but not in right. - it
   * will output MAX(m - n, 0) elements of left for all elements which are present in both left and
   * right.
   *
   * <p>Note that this transform requires that the {@code Coder} of the all {@code PCollection<T>}
   * to be deterministic (see {@link Coder#verifyDeterministic()}). If the collection {@code Coder}
   * is not deterministic, an exception is thrown at pipeline construction time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}).Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the first {@code PCollection<T>} in {@code PCollectionList<T>}.
   *
   * <pre>{@code
   * Pipeline p = ...;
   * PCollection<String> first = p.apply(Create.of("1", "1", "1", "2", "3", "3", "3", "4", "5"));
   * PCollection<String> second = p.apply(Create.of("1", "3", "4", "4", "6"));
   * PCollection<String> third = p.apply(Create.of("1", "5"));
   *
   * // Following example will perform (first intersect second) intersect third.
   * PCollection<String> results =
   *     PCollectionList.of(first).and(second).and(third)
   *     .apply(SetFns.exceptAll()); // results will be PCollection<String> containing: "1","2","3","3"
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input {@code PCollectionList<T>} and output {@code
   *     PCollection<T>}s.
   */
  public static <T> PTransform<PCollectionList<T>, PCollection<T>> exceptAll() {
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
   * <p>Note that this transform requires that the {@code Coder} of the all {@code PCollection<T>}
   * to be deterministic (see {@link Coder#verifyDeterministic()}). If the collection {@code Coder}
   * is not deterministic, an exception is thrown at pipeline construction time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}).Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the input {@code PCollection<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "1", "2"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4", "4"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.unionDistinct(right)); // results will be PCollection<String> containing: "1","2","3","4"
   * }</pre>
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> PTransform<PCollection<T>, PCollection<T>> unionDistinct(
      PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, unionDistinct());
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET DISTINCT semantics which takes a
   * {@code PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing the
   * union of collections done in order for all collections in {@code PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} will have all distinct elements that are
   * present in pipeline is constructed or present in next {@link PCollection} in the list and
   * applied to all collections in order.
   *
   * <p>Note that this transform requires that the {@code Coder} of the all {@code PCollection<T>}
   * to be deterministic (see {@link Coder#verifyDeterministic()}). If the collection {@code Coder}
   * is not deterministic, an exception is thrown at pipeline construction time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}).Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the first {@code PCollection<T>} in {@code PCollectionList<T>}.
   *
   * <pre>{@code
   * Pipeline p = ...;
   * PCollection<String> first = p.apply(Create.of("1", "1", "2"));
   * PCollection<String> second = p.apply(Create.of("1", "3", "4", "4"));
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
  public static <T> PTransform<PCollectionList<T>, PCollection<T>> unionDistinct() {
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
   * <p>Note that this transform requires that the {@code Coder} of the all {@code PCollection<T>}
   * to be deterministic (see {@link Coder#verifyDeterministic()}). If the collection {@code Coder}
   * is not deterministic, an exception is thrown at pipeline construction time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}).Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the input {@code PCollection<T>}
   *
   * <pre>{@code
   * Pipeline p = ...;
   *
   * PCollection<String> left = p.apply(Create.of("1", "1", "2"));
   * PCollection<String> right = p.apply(Create.of("1", "3", "4", "4"));
   *
   * PCollection<String> results =
   *     left.apply(SetFns.unionAll(right)); // results will be PCollection<String> containing: "1","1","1","2","3","4","4"
   * }</pre>
   *
   * @param <T> the type of the elements in the input and output {@code PCollection<T>}s.
   */
  public static <T> PTransform<PCollection<T>, PCollection<T>> unionAll(
      PCollection<T> rightCollection) {
    checkNotNull(rightCollection, "rightCollection argument is null");
    return new SetImpl<>(rightCollection, unionAll());
  }

  /**
   * Returns a new {@code PTransform} transform that follows SET ALL semantics which takes a {@code
   * PCollectionList<PCollection<T>>} and returns a {@code PCollection<T>} containing the unionAll
   * of collections done in order for all collections in {@code PCollectionList<T>}.
   *
   * <p>The elements of the output {@link PCollection} which will follow UNION_ALL semantics. Output
   * is calculated as follows: Given there are m elements on pipeline which is constructed {@link
   * PCollection} (left) and n elements on in provided {@link PCollection} (right): - it will output
   * m elements of left and m elements of right.
   *
   * <p>Note that this transform requires that the {@code Coder} of the all inputs {@code
   * PCollection<T>} to be deterministic (see {@link Coder#verifyDeterministic()}). If the
   * collection {@code Coder} is not deterministic, an exception is thrown at pipeline construction
   * time.
   *
   * <p>All inputs must have equal {@link WindowFn}s and compatible triggers (see {@link
   * Trigger#isCompatible(Trigger)}).Triggers with multiple firings may lead to nondeterministic
   * results since the this {@code PTransform} is only computed over each individual firing.
   *
   * <p>By default, the output {@code PCollection<T>} encodes its elements using the same {@code
   * Coder} as that of the first {@code PCollection<T>} in {@code PCollectionList<T>}.
   *
   * <pre>{@code
   * Pipeline p = ...;
   * PCollection<String> first = p.apply(Create.of("1", "1", "2"));
   * PCollection<String> second = p.apply(Create.of("1", "3", "4", "4"));
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

    private final SerializableBiFunction<Long, Long, Long> fn;

    private SetImplCollections(SerializableBiFunction<Long, Long, Long> fn) {
      this.fn = fn;
    }

    @Override
    public PCollection<T> expand(PCollectionList<T> input) {
      List<PCollection<T>> all = input.getAll();
      MapElements<T, KV<T, Void>> elementToVoid =
          MapElements.via(
              new SimpleFunction<T, KV<T, Void>>() {
                @Override
                public KV<T, Void> apply(T element) {
                  return KV.of(element, null);
                }
              });

      checkArgument(all.size() > 1, "must have at least two input to a PCollectionList");

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
  }
}
