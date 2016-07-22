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
package org.apache.beam.sdk.extensions.joinlibrary;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Utility class with different versions of joins. All methods join two collections of
 * key/value pairs (KV).
 */
public class Join {

  /**
   * Inner join of two collections of KV elements.
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a
   *         KV where Key is of type V1 and Value is type V2.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> innerJoin(
    final PCollection<KV<K, V1>> leftCollection, final PCollection<KV<K, V2>> rightCollection) {
    checkNotNull(leftCollection);
    checkNotNull(rightCollection);

    final TupleTag<V1> v1Tuple = new TupleTag<>();
    final TupleTag<V2> v2Tuple = new TupleTag<>();

    PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
      KeyedPCollectionTuple.of(v1Tuple, leftCollection)
        .and(v2Tuple, rightCollection)
        .apply(CoGroupByKey.<K>create());

    return coGbkResultCollection.apply(ParDo.of(
      new OldDoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
        @Override
        public void processElement(ProcessContext c) {
          KV<K, CoGbkResult> e = c.element();

          Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
          Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

          for (V1 leftValue : leftValuesIterable) {
            for (V2 rightValue : rightValuesIterable) {
              c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
            }
          }
        }
      }))
      .setCoder(KvCoder.of(((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                           KvCoder.of(((KvCoder) leftCollection.getCoder()).getValueCoder(),
                                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
  }

  /**
   * Left Outer Join of two collections of KV elements.
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param nullValue Value to use as null value when right side do not match left side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a
   *         KV where Key is of type V1 and Value is type V2. Values that
   *         should be null or empty is replaced with nullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> leftOuterJoin(
    final PCollection<KV<K, V1>> leftCollection,
    final PCollection<KV<K, V2>> rightCollection,
    final V2 nullValue) {
    checkNotNull(leftCollection);
    checkNotNull(rightCollection);
    checkNotNull(nullValue);

    final TupleTag<V1> v1Tuple = new TupleTag<>();
    final TupleTag<V2> v2Tuple = new TupleTag<>();

    PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
      KeyedPCollectionTuple.of(v1Tuple, leftCollection)
        .and(v2Tuple, rightCollection)
        .apply(CoGroupByKey.<K>create());

    return coGbkResultCollection.apply(ParDo.of(
      new OldDoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
        @Override
        public void processElement(ProcessContext c) {
          KV<K, CoGbkResult> e = c.element();

          Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
          Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

          for (V1 leftValue : leftValuesIterable) {
            if (rightValuesIterable.iterator().hasNext()) {
              for (V2 rightValue : rightValuesIterable) {
                c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
              }
            } else {
              c.output(KV.of(e.getKey(), KV.of(leftValue, nullValue)));
            }
          }
        }
      }))
      .setCoder(KvCoder.of(((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                           KvCoder.of(((KvCoder) leftCollection.getCoder()).getValueCoder(),
                                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
  }

  /**
   * Right Outer Join of two collections of KV elements.
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param nullValue Value to use as null value when left side do not match right side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a
   *         KV where Key is of type V1 and Value is type V2. Keys that
   *         should be null or empty is replaced with nullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> rightOuterJoin(
    final PCollection<KV<K, V1>> leftCollection,
    final PCollection<KV<K, V2>> rightCollection,
    final V1 nullValue) {
    checkNotNull(leftCollection);
    checkNotNull(rightCollection);
    checkNotNull(nullValue);

    final TupleTag<V1> v1Tuple = new TupleTag<>();
    final TupleTag<V2> v2Tuple = new TupleTag<>();

    PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
      KeyedPCollectionTuple.of(v1Tuple, leftCollection)
        .and(v2Tuple, rightCollection)
        .apply(CoGroupByKey.<K>create());

    return coGbkResultCollection.apply(ParDo.of(
      new OldDoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
        @Override
        public void processElement(ProcessContext c) {
          KV<K, CoGbkResult> e = c.element();

          Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
          Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

          for (V2 rightValue : rightValuesIterable) {
            if (leftValuesIterable.iterator().hasNext()) {
              for (V1 leftValue : leftValuesIterable) {
                c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
              }
            } else {
              c.output(KV.of(e.getKey(), KV.of(nullValue, rightValue)));
            }
          }
        }
      }))
      .setCoder(KvCoder.of(((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                           KvCoder.of(((KvCoder) leftCollection.getCoder()).getValueCoder(),
                                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
  }
}
