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

import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.joda.time.Duration;
import org.joda.time.Instant;

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
                  ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder())));
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
                  ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder())));
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
                  ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder())));
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
                  ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder(),
                  KvCoder.of(
                      ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder(),
                      ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder())));
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
   * PTransform representing an equijoin of PCollection<KV>s bounded by event time.
   *
   * @param <K> Type of the key for both collections.
   * @param <V1> Type of the values for the left collection.
   * @param <V2> Type of the values for the right collection.
   */
  public static class EventTimeBoundedEquijoin<K, V1, V2>
      extends PTransform<PCollection<KV<K, V1>>, PCollection<KV<K, KV<V1, V2>>>> {
    private final transient PCollection<KV<K, V2>> rightCollection;
    private final Duration temporalBound;

    private EventTimeBoundedEquijoin(
        final PCollection<KV<K, V2>> rightCollection, final Duration temporalBound) {
      this.temporalBound = temporalBound;
      this.rightCollection = rightCollection;
    }

    /**
     * Returns a EventTimeBoundedEquijoin PTransform that joins two PCollection<KV>s.
     *
     * <p>Similar to {@link #innerJoin} but also supports unbounded PCollections in the
     * GlobalWindow. Join results will be produced eagerly as new elements are received, regardless
     * of windowing, however users should prefer {@link #innerJoin} in most cases for better
     * throughput.
     *
     * <p>The non-inclusive {@code temporalBound}, used as part of the join predicate, allows
     * elements to be expired when they are irrelevant according to the event-time watermark. This
     * helps reduce the search space, storage, and memory requirements.
     *
     * @param rightCollection Right side collection of the join.
     * @param temporalBound Duration used in the join predicate (non-inclusive).
     * @param <K> Type of the key for both collections.
     * @param <V1> Type of the values for the left collection.
     * @param <V2> Type of values for the right collection.
     */
    public static <K, V1, V2> EventTimeBoundedEquijoin<K, V1, V2> with(
        PCollection<KV<K, V2>> rightCollection, Duration temporalBound) {
      return new EventTimeBoundedEquijoin<>(rightCollection, temporalBound);
    }

    @Override
    public PCollection<KV<K, KV<V1, V2>>> expand(PCollection<KV<K, V1>> leftCollection) {
      // left        right
      // tag-left    tag-right (create union type)
      //   \         /
      //     flatten
      //     join

      Coder<K> keyCoder = ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder();
      Coder<V1> leftValueCoder = ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder();
      Coder<V2> rightValueCoder = ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder();

      PCollection<KV<K, KV<V1, V2>>> leftUnion =
          leftCollection
              .apply("LeftUnionTag", MapElements.via(new LeftUnionTagFn<K, V1, V2>()))
              .setCoder(
                  KvCoder.of(
                      keyCoder,
                      KvCoder.of(
                          NullableCoder.of(leftValueCoder), NullableCoder.of(rightValueCoder))));

      PCollection<KV<K, KV<V1, V2>>> rightUnion =
          rightCollection
              .apply("RightUnionTag", MapElements.via(new RightUnionTagFn<K, V1, V2>()))
              .setCoder(
                  KvCoder.of(
                      keyCoder,
                      KvCoder.of(
                          NullableCoder.of(leftValueCoder), NullableCoder.of(rightValueCoder))));

      return PCollectionList.of(leftUnion)
          .and(rightUnion)
          .apply("FlattenUnionTagged", Flatten.pCollections())
          .apply(
              "EventTimeBoundedEquijoinFn",
              ParDo.of(new EventTimeEquijoinFn<>(leftValueCoder, rightValueCoder, temporalBound)));
    }
  }

  private static class LeftUnionTagFn<K, V1, V2>
      extends SimpleFunction<KV<K, V1>, KV<K, KV<V1, V2>>> {
    @Override
    public KV<K, KV<V1, V2>> apply(KV<K, V1> element) {
      return KV.of(element.getKey(), KV.of(element.getValue(), null));
    }
  }

  private static class RightUnionTagFn<K, V1, V2>
      extends SimpleFunction<KV<K, V2>, KV<K, KV<V1, V2>>> {
    @Override
    public KV<K, KV<V1, V2>> apply(KV<K, V2> element) {
      return KV.of(element.getKey(), KV.of(null, element.getValue()));
    }
  }

  private static class EventTimeEquijoinFn<K, V1, V2>
      extends DoFn<KV<K, KV<V1, V2>>, KV<K, KV<V1, V2>>> {
    private static final String LEFT_STATE = "left";
    private static final String RIGHT_STATE = "right";
    private static final String LAST_EVICTION_STATE = "lastEviction";
    private static final String EVICTION_TIMER_INIT_STATE = "evictionTimerInit";
    private static final String EVICTION_TIMER = "eviction";

    @StateId(LEFT_STATE)
    private final StateSpec<OrderedListState<V1>> leftStateSpec;

    @StateId(RIGHT_STATE)
    private final StateSpec<OrderedListState<V2>> rightStateSpec;

    // Null only when uninitialized. After first element is received this will always be non-null.
    @StateId(LAST_EVICTION_STATE)
    private final StateSpec<ValueState<Instant>> lastEvictionStateSpec;

    // Tracks the initialization state of the eviction timer. Value is true when the timer has been
    // set and execution is waiting for the event time watermark to fire the timer according to the
    // evictionFrequency. False after the timer has been fired, so processElement can set the timer
    // using the previous firing event time.
    @StateId(EVICTION_TIMER_INIT_STATE)
    private final StateSpec<ValueState<Boolean>> evictionTimerInitSpec;

    @TimerId(EVICTION_TIMER)
    private final TimerSpec evictionSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private final Duration temporalBound;
    private final Duration evictionFrequency;

    protected EventTimeEquijoinFn(
        final Coder<V1> leftCoder, final Coder<V2> rightCoder, final Duration temporalBound) {
      this.leftStateSpec = StateSpecs.orderedList(leftCoder);
      this.rightStateSpec = StateSpecs.orderedList(rightCoder);
      this.lastEvictionStateSpec = StateSpecs.value(InstantCoder.of());
      this.evictionTimerInitSpec = StateSpecs.value(BooleanCoder.of());
      this.temporalBound = temporalBound;
      this.evictionFrequency =
          temporalBound.getMillis() <= 4 ? Duration.millis(1) : temporalBound.dividedBy(4);
    }

    @ProcessElement
    public void processElement(
        @Element KV<K, KV<V1, V2>> element,
        OutputReceiver<KV<K, KV<V1, V2>>> outputReceiver,
        @StateId(LEFT_STATE) OrderedListState<V1> leftState,
        @StateId(RIGHT_STATE) OrderedListState<V2> rightState,
        @AlwaysFetched @StateId(LAST_EVICTION_STATE) ValueState<Instant> lastEvictionState,
        @AlwaysFetched @StateId(EVICTION_TIMER_INIT_STATE)
            ValueState<Boolean> evictionTimerSetState,
        @Timestamp Instant timestamp,
        @TimerId(EVICTION_TIMER) Timer evictionTimer) {
      Instant lastEviction = lastEvictionState.read();
      if (lastEviction == null) {
        // Initialize timer for the first time relatively since event time watermark is unknown.
        evictionTimerSetState.write(true);
        evictionTimer.offset(evictionFrequency).setRelative();
      } else if (!MoreObjects.firstNonNull(evictionTimerSetState.read(), false)) {
        // Set timer using persisted event watermark from last timer firing event time.
        checkNotNull(lastEviction);
        evictionTimerSetState.write(true);
        evictionTimer.set(lastEviction.plus(evictionFrequency));
      }

      K key = element.getKey();
      V1 left = element.getValue().getKey();
      V2 right = element.getValue().getValue();
      if (left != null) {
        leftState.add(TimestampedValue.of(left, timestamp));
        rightState
            .readRange(timestamp.minus(temporalBound), timestamp.plus(temporalBound))
            .forEach(
                r -> {
                  if (isAbsoluteDurationShorterThanTemporalBound(r.getTimestamp(), timestamp)) {
                    outputReceiver.output(KV.of(key, KV.of(left, r.getValue())));
                  }
                });
      } else {
        rightState.add(TimestampedValue.of(right, timestamp));
        leftState
            .readRange(timestamp.minus(temporalBound), timestamp.plus(temporalBound))
            .forEach(
                l -> {
                  if (isAbsoluteDurationShorterThanTemporalBound(l.getTimestamp(), timestamp)) {
                    outputReceiver.output(KV.of(key, KV.of(l.getValue(), right)));
                  }
                });
      }
    }

    private boolean isAbsoluteDurationShorterThanTemporalBound(Instant time1, Instant time2) {
      return new Duration(time1, time2).abs().isShorterThan(temporalBound);
    }

    @OnTimer(EVICTION_TIMER)
    public void onEviction(
        @StateId(LEFT_STATE) OrderedListState<V1> leftState,
        @StateId(RIGHT_STATE) OrderedListState<V2> rightState,
        @StateId(LAST_EVICTION_STATE) ValueState<Instant> lastEvictionState,
        @StateId(EVICTION_TIMER_INIT_STATE) ValueState<Boolean> evictionTimerSetState,
        @Timestamp Instant ts) {
      evictionTimerSetState.write(false);
      lastEvictionState.write(ts);
      leftState.clearRange(BoundedWindow.TIMESTAMP_MIN_VALUE, ts.minus(temporalBound));
      rightState.clearRange(BoundedWindow.TIMESTAMP_MIN_VALUE, ts.minus(temporalBound));
    }
  }

  /**
   * Inner joins two PCollection<KV>s within a bounded event time duration.
   *
   * <p>Similar to {@code innerJoin} but also supports unbounded PCollections in the GlobalWindow.
   * Join results will be produced eagerly as new elements are received, regardless of windowing.
   *
   * <p>The non-inclusive {@code temporalBound}, used as part of the join predicate, allows elements
   * to be expired when they are irrelevant according to the event-time watermark. This helps reduce
   * the search space, storage, and memory requirements.
   *
   * @param <K> Join key type.
   * @param <V1> Left element type in the left collection.
   * @param <V2> Right element type in the right collection.
   * @param name Name of the PTransform.
   * @param leftCollection Left collection of the join.
   * @param rightCollection Right collection of the join.
   * @param temporalBound Time domain range used in the join predicate (non-inclusive).
   */
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> eventTimeBoundedInnerjoin(
      final String name,
      final PCollection<KV<K, V1>> leftCollection,
      final PCollection<KV<K, V2>> rightCollection,
      final Duration temporalBound) {
    return leftCollection
        .apply(name, EventTimeBoundedEquijoin.with(rightCollection, temporalBound))
        .setCoder(
            KvCoder.of(
                ((KvCoder<K, V1>) leftCollection.getCoder()).getKeyCoder(),
                KvCoder.of(
                    ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder(),
                    ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder())));
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
