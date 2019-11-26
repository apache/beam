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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Utility class with different versions of joins. All methods join two collections of key/value
 * pairs (KV).
 */
public class Join {

  /**
   * PTransform representing an inner join of two collections of KV elements.
   *
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   */
  public static class InnerJoin<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, KV<V1, V2>>>> {

    private transient PCollection<KV<K, V2>> rightCollection;

    private InnerJoin(PCollection<KV<K, V2>> rightCollection) {
      this.rightCollection = rightCollection;
    }

    public static <K, V1, V2> InnerJoin<K, V1, V2> with(PCollection<KV<K, V2>> rightCollection) {
      return new InnerJoin<>(rightCollection);
    }

    @Override
    public PCollection<KV<K, KV<V1, V2>>> expand(PCollection<KV<K, V1>> leftCollection) {
      checkNotNull(leftCollection);
      checkNotNull(rightCollection);

      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
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
          .setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
  }

  /**
   * PTransform representing a left outer join of two collections of KV elements.
   *
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   */
  public static class LeftOuterJoin<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, KV<V1, V2>>>> {

    private transient PCollection<KV<K, V2>> rightCollection;
    private V2 nullValue;

    private LeftOuterJoin(PCollection<KV<K, V2>> rightCollection, V2 nullValue) {
      this.rightCollection = rightCollection;
      this.nullValue = nullValue;
    }

    public static <K, V1, V2> LeftOuterJoin<K, V1, V2> with(
        PCollection<KV<K, V2>> rightCollection, V2 nullValue) {
      return new LeftOuterJoin<>(rightCollection, nullValue);
    }

    @Override
    public PCollection<KV<K, KV<V1, V2>>> expand(PCollection<KV<K, V1>> leftCollection) {
      checkNotNull(leftCollection);
      checkNotNull(rightCollection);
      checkNotNull(nullValue);
      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
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
          .setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
  }

  /**
   * PTransform representing a right outer join of two collections of KV elements.
   *
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   */
  public static class RightOuterJoin<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, KV<V1, V2>>>> {

    private transient PCollection<KV<K, V2>> rightCollection;
    private V1 nullValue;

    private RightOuterJoin(PCollection<KV<K, V2>> rightCollection, V1 nullValue) {
      this.rightCollection = rightCollection;
      this.nullValue = nullValue;
    }

    public static <K, V1, V2> RightOuterJoin<K, V1, V2> with(
        PCollection<KV<K, V2>> rightCollection, V1 nullValue) {
      return new RightOuterJoin<>(rightCollection, nullValue);
    }

    @Override
    public PCollection<KV<K, KV<V1, V2>>> expand(PCollection<KV<K, V1>> leftCollection) {
      checkNotNull(leftCollection);
      checkNotNull(rightCollection);
      checkNotNull(nullValue);

      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
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
          .setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
  }

  /**
   * PTransform representing a full outer join of two collections of KV elements.
   *
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   */
  public static class FullOuterJoin<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, KV<V1, V2>>>> {

    private transient PCollection<KV<K, V2>> rightCollection;
    private V1 leftNullValue;
    private V2 rightNullValue;

    private FullOuterJoin(
        PCollection<KV<K, V2>> rightCollection, V1 leftNullValue, V2 rightNullValue) {
      this.rightCollection = rightCollection;
      this.leftNullValue = leftNullValue;
      this.rightNullValue = rightNullValue;
    }

    public static <K, V1, V2> FullOuterJoin<K, V1, V2> with(
        PCollection<KV<K, V2>> rightCollection, V1 leftNullValue, V2 rightNullValue) {
      return new FullOuterJoin<>(rightCollection, leftNullValue, rightNullValue);
    }

    @Override
    public PCollection<KV<K, KV<V1, V2>>> expand(PCollection<KV<K, V1>> leftCollection) {
      checkNotNull(leftCollection);
      checkNotNull(rightCollection);
      checkNotNull(leftNullValue);
      checkNotNull(rightNullValue);

      final TupleTag<V1> v1Tuple = new TupleTag<>();
      final TupleTag<V2> v2Tuple = new TupleTag<>();

      PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(v1Tuple, leftCollection)
              .and(v2Tuple, rightCollection)
              .apply("CoGBK", CoGroupByKey.create());

      return coGbkResultCollection
          .apply(
              "Join",
              ParDo.of(
                  new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      KV<K, CoGbkResult> e = c.element();

                      Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                      Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);
                      if (leftValuesIterable.iterator().hasNext()
                          && rightValuesIterable.iterator().hasNext()) {
                        for (V2 rightValue : rightValuesIterable) {
                          for (V1 leftValue : leftValuesIterable) {
                            c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                          }
                        }
                      } else if (leftValuesIterable.iterator().hasNext()
                          && !rightValuesIterable.iterator().hasNext()) {
                        for (V1 leftValue : leftValuesIterable) {
                          c.output(KV.of(e.getKey(), KV.of(leftValue, rightNullValue)));
                        }
                      } else if (!leftValuesIterable.iterator().hasNext()
                          && rightValuesIterable.iterator().hasNext()) {
                        for (V2 rightValue : rightValuesIterable) {
                          c.output(KV.of(e.getKey(), KV.of(leftNullValue, rightValue)));
                        }
                      }
                    }
                  }))
          .setCoder(
              KvCoder.of(
                  ((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
  }

  /**
   * Inner join of two collections of KV elements.
   *
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> innerJoin(
      final PCollection<KV<K, V1>> leftCollection, final PCollection<KV<K, V2>> rightCollection) {
    return innerJoin("InnerJoin", leftCollection, rightCollection);
  }

  /**
   * Inner join of two collections of KV elements.
   *
   * @param name Name of the PTransform.
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> innerJoin(
      final String name,
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection) {
    return leftCollection.apply(name, InnerJoin.with(rightCollection));
  }

  /**
   * Left Outer Join of two collections of KV elements.
   *
   * @param name Name of the PTransform.
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param nullValue Value to use as null value when right side do not match left side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2. Values that should be null or empty is replaced with nullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> leftOuterJoin(
      final String name,
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V2 nullValue) {
    return leftCollection.apply(name, LeftOuterJoin.with(rightCollection, nullValue));
  }

  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> leftOuterJoin(
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V2 nullValue) {
    return leftOuterJoin("LeftOuterJoin", leftCollection, rightCollection, nullValue);
  }

  /**
   * Right Outer Join of two collections of KV elements.
   *
   * @param name Name of the PTransform.
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param nullValue Value to use as null value when left side do not match right side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2. Values that should be null or empty is replaced with nullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> rightOuterJoin(
      final String name,
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V1 nullValue) {
    return leftCollection.apply(name, RightOuterJoin.with(rightCollection, nullValue));
  }

  /**
   * Right Outer Join of two collections of KV elements.
   *
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param nullValue Value to use as null value when left side do not match right side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2. Values that should be null or empty is replaced with nullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> rightOuterJoin(
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V1 nullValue) {
    return rightOuterJoin("RightOuterJoin", leftCollection, rightCollection, nullValue);
  }

  /**
   * Full Outer Join of two collections of KV elements.
   *
   * @param name Name of the PTransform.
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param leftNullValue Value to use as null value when left side do not match right side.
   * @param rightNullValue Value to use as null value when right side do not match right side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2. Values that should be null or empty is replaced with
   *     leftNullValue/rightNullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> fullOuterJoin(
      final String name,
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V1 leftNullValue,
      final V2 rightNullValue) {
    return leftCollection.apply(
        name, FullOuterJoin.with(rightCollection, leftNullValue, rightNullValue));
  }

  /**
   * Full Outer Join of two collections of KV elements.
   *
   * @param leftCollection Left side collection to join.
   * @param rightCollection Right side collection to join.
   * @param leftNullValue Value to use as null value when left side do not match right side.
   * @param rightNullValue Value to use as null value when right side do not match right side.
   * @param <K> Type of the key for both collections
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   * @return A joined collection of KV where Key is the key and value is a KV where Key is of type
   *     V1 and Value is type V2. Values that should be null or empty is replaced with
   *     leftNullValue/rightNullValue.
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> fullOuterJoin(
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final V1 leftNullValue,
      final V2 rightNullValue) {
    return fullOuterJoin(
        "FullOuterJoin", leftCollection, rightCollection, leftNullValue, rightNullValue);
  }
}
