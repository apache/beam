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
package org.apache.beam.sdk.transforms.join;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.PeekingIterator;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * This class allows joining two {@link PCollection}s using temporal conditions.
 *
 * <p>Several types of joins are supported:
 *
 * <p><b>As Of Joins</b> Join each element of the main PCollection with the element/s of the
 * right-hand PCollection having the closest timestamp before the main element's timestamp.
 *
 * <p>For example, consider two PCollections representing currency trades and quotes. For each trade
 * we want to find the most-recent quote for that currency that has a timestamp earlier than the
 * trade; this will give us the currency price at the time of the trade.
 *
 * <pre>{@code
 * PCollection<KV<Currency, Double>> quotes = readQuotes();
 * PCollection<KV<Currency, Trade>> trades = readTrades();
 * PCollection<Trade> tradesWithPrices =
 *   trades.apply(TemporalJoin.innerJoin(quotes))
 *         .apply(Values.create())
 *         .apply(MapElements.to(trades()).via(result -> result.getMainElement().getValue().withPrice(result.getAgainstElements().get(0))));
 * }</pre>
 *
 * <b>Within Joins</b> Join each element of the main PCollection with all elements of the right-hand
 * PCollection that happened within a specified duration before the main element's timestamp. For
 * example:
 *
 * <pre>{@code
 * trades.apply(TemporalJoin.innerJoin(quotes).within(Duration.standardHours(1)));
 * }</pre>
 *
 * Will return all quotes that happened within the hour before each trade.
 *
 * <p>The PCollection's built-in timestamp dimension is used for all of these joins. The user can
 * offset the join timestamp used using the first parameter to {@link Impl#asOf(Duration)} or {@link
 * Impl#within(Duration, Duration)}.
 *
 * <p>Left outer joins are supported using {@link #leftJoin} Right or full outer joins are not
 * supported.
 *
 * <p>{@link Impl#withMaximumJoinPeriod} can be used to specify that a main element should never
 * join a right-hand element that is too far in the past. It is recommended that this parameter be
 * set; if not set, some state will never be garbage collected, possibly causing performance
 * problems.
 */
public class TemporalJoin {
  /**
   * Result class from TemporalJoin.
   *
   * <p>Every item from the main PCollection can join against some number of items from the rhs
   * PCollection. Items always join against the the item with the closest timestamp; if there are
   * multiple items with the same closest timestamp, all of the will be in getAgainstElements. If
   * {@link #leftJoin} is used and a main item did not join before its watermark expired, then it
   * will be output with an empty rhsElements collection.
   */
  public static class Result<MainT, AgainstT> {
    private final TimestampedValue<MainT> leftElement;
    // In the case where there are multiple elements with the same timestamp that match, this
    // iterable will contain those values.
    private final Iterable<TimestampedValue<AgainstT>> rhsElements;

    private Result(
        TimestampedValue<MainT> leftElement, Iterable<TimestampedValue<AgainstT>> rhsElements) {
      this.leftElement = leftElement;
      this.rhsElements = rhsElements;
    }

    public static <MainT, AgainstT> Result<MainT, AgainstT> of(
        TimestampedValue<MainT> leftElement, Iterable<TimestampedValue<AgainstT>> rhsElements) {
      return new Result<>(leftElement, rhsElements);
    }

    public TimestampedValue<MainT> getLeftElement() {
      return leftElement;
    }

    public Iterable<TimestampedValue<AgainstT>> getRhsElements() {
      return rhsElements;
    }
  }

  public static class ResultCoder<MainT, AgainstT>
      extends StructuredCoder<Result<MainT, AgainstT>> {
    private final Coder<TimestampedValue<MainT>> leftCoder;
    private final Coder<Iterable<TimestampedValue<AgainstT>>> rhsCoder;

    private ResultCoder(
        Coder<TimestampedValue<MainT>> leftCoder,
        Coder<Iterable<TimestampedValue<AgainstT>>> rhsCoder) {
      this.leftCoder = leftCoder;
      this.rhsCoder = rhsCoder;
    }

    public static <MainT, AgainstT> ResultCoder<MainT, AgainstT> of(
        Coder<MainT> mainCoder, Coder<AgainstT> rhsCoder) {
      return new ResultCoder<>(
          TimestampedValueCoder.of(mainCoder),
          IterableCoder.of(TimestampedValueCoder.of(rhsCoder)));
    }

    @Override
    public void encode(Result<MainT, AgainstT> value, OutputStream outStream)
        throws CoderException, IOException {
      leftCoder.encode(value.getLeftElement(), outStream);
      rhsCoder.encode(value.getRhsElements(), outStream);
    }

    @Override
    public Result<MainT, AgainstT> decode(InputStream inStream) throws CoderException, IOException {
      return Result.of(leftCoder.decode(inStream), rhsCoder.decode(inStream));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return ImmutableList.of(leftCoder, rhsCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      leftCoder.verifyDeterministic();
      rhsCoder.verifyDeterministic();
    }
  }

  public static <K, MaintT, AgainstT> Impl<K, MaintT, AgainstT> innerJoin(
      PCollection<KV<K, AgainstT>> rhsCollection) {
    return new AutoValue_TemporalJoin_Impl.Builder<K, MaintT, AgainstT>()
        .setRightHandCollection(rhsCollection)
        .setOutputUnjoinedElements(false)
        .build();
  }

  public static <K, MaintT, AgainstT> Impl<K, MaintT, AgainstT> leftJoin(
      PCollection<KV<K, AgainstT>> rhsCollection) {
    return new AutoValue_TemporalJoin_Impl.Builder<K, MaintT, AgainstT>()
        .setRightHandCollection(rhsCollection)
        .setOutputUnjoinedElements(true)
        .setAsOfOffset(Duration.ZERO)
        .build();
  }

  @AutoValue
  public abstract static class Impl<K, MainT, AgainstT>
      extends PTransform<PCollection<KV<K, MainT>>, PCollection<KV<K, Result<MainT, AgainstT>>>> {
    abstract PCollection<KV<K, AgainstT>> getRightHandCollection();

    abstract boolean getOutputUnjoinedElements();

    abstract Duration getAsOfOffset();

    @Nullable
    abstract Duration getWithin();

    @Nullable
    abstract Duration getMaximumJoinPeriod();

    abstract Builder<K, MainT, AgainstT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, MainT, AgainstT> {
      abstract Builder<K, MainT, AgainstT> setRightHandCollection(
          PCollection<KV<K, AgainstT>> rhsCollection);

      abstract Builder<K, MainT, AgainstT> setOutputUnjoinedElements(
          boolean outputUnjoinedElements);

      abstract Builder<K, MainT, AgainstT> setMaximumJoinPeriod(
          @Nullable Duration maximumJoinPeriod);

      abstract Builder<K, MainT, AgainstT> setAsOfOffset(Duration asOfOffset);

      abstract Builder<K, MainT, AgainstT> setWithin(Duration within);

      abstract Impl<K, MainT, AgainstT> build();
    }

    public Impl<K, MainT, AgainstT> withMaximumJoinPeriod(Duration maximumJoinPeriod) {
      return toBuilder().setMaximumJoinPeriod(maximumJoinPeriod).build();
    }

    public Impl<K, MainT, AgainstT> asOf(Duration asOfOffset) {
      return toBuilder().setAsOfOffset(Duration.ZERO).setWithin(null).build();
    }

    public Impl<K, MainT, AgainstT> asOf() {
      return toBuilder().setAsOfOffset(Duration.ZERO).setWithin(null).build();
    }

    public Impl<K, MainT, AgainstT> within(Duration asOfOffset, Duration range) {
      return toBuilder().setAsOfOffset(asOfOffset).setWithin(range).build();
    }

    public Impl<K, MainT, AgainstT> within(Duration range) {
      return toBuilder().setAsOfOffset(Duration.ZERO).setWithin(range).build();
    }

    @Override
    public PCollection<KV<K, Result<MainT, AgainstT>>> expand(PCollection<KV<K, MainT>> input) {
      Coder<K> keyCoder = getKeyCoder(input.getCoder());
      Coder<MainT> mainValueCoder = getValueCoder(input.getCoder());
      Coder<AgainstT> rhsValueCoder = getValueCoder(getRightHandCollection().getCoder());
      UnionCoder unionCoder = UnionCoder.of(ImmutableList.of(mainValueCoder, rhsValueCoder));
      KvCoder<K, RawUnionValue> kvCoder = KvCoder.of(getKeyCoder(input.getCoder()), unionCoder);
      PCollectionList<KV<K, RawUnionValue>> union =
          PCollectionList.of(makeUnionTable(0, input, kvCoder))
              .and(makeUnionTable(1, getRightHandCollection(), kvCoder));
      return union
          .apply("Flatten", Flatten.pCollections())
          .apply(
              "Join",
              ParDo.of(
                  new JoinDoFn<>(
                      mainValueCoder,
                      rhsValueCoder,
                      getOutputUnjoinedElements(),
                      getAsOfOffset(),
                      getWithin(),
                      getMaximumJoinPeriod())))
          .setCoder(KvCoder.of(keyCoder, ResultCoder.of(mainValueCoder, rhsValueCoder)));
    }
  }

  private static class JoinDoFn<K, MainT, AgainstT>
      extends DoFn<KV<K, RawUnionValue>, KV<K, Result<MainT, AgainstT>>> {
    private static final int MAIN_TAG = 0;
    private static final int AGAINST_TAG = 1;
    // Bucket timers to the nearest minute. This might be something users want to configure in the
    // future.
    private static final long TIMER_BUCKET = Duration.standardMinutes(1).getMillis();
    private final Coder<MainT> mainValueCoder;
    private final Coder<AgainstT> rhsValueCoder;
    private final boolean outputUnjoinedElements;
    private final Duration asOfOffset;
    @Nullable private final Duration within;
    @Nullable private final Duration maximumJoinPeriod;

    @StateId("mainItems")
    private final StateSpec<OrderedListState<MainT>> mainItems;

    @StateId("rhsItems")
    private final StateSpec<OrderedListState<AgainstT>> rhsItems;

    @TimerFamily("joinTimers")
    private final TimerSpec joinTimers = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    @TimerFamily("cleanupTimers")
    private final TimerSpec cleanupTimers = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    public JoinDoFn(
        Coder<MainT> mainValueCoder,
        Coder<AgainstT> rhsValueCoder,
        boolean outputUnjoinedElements,
        Duration asOfOffset,
        Duration within,
        Duration maximumJoinPeriod) {
      this.mainValueCoder = mainValueCoder;
      this.rhsValueCoder = rhsValueCoder;
      this.mainItems = StateSpecs.orderedList(mainValueCoder);
      this.rhsItems = StateSpecs.orderedList(rhsValueCoder);
      this.outputUnjoinedElements = outputUnjoinedElements;
      this.asOfOffset = asOfOffset;
      this.within = within;
      this.maximumJoinPeriod = maximumJoinPeriod;
    }

    private static Instant bucketTimestamp(Instant ts) {
      long bucketIndex = ts.getMillis() / TIMER_BUCKET;
      return Instant.ofEpochMilli(bucketIndex * TIMER_BUCKET);
    }

    @ProcessElement
    public void process(
        @Element KV<K, RawUnionValue> element,
        @Timestamp Instant ts,
        @StateId("mainItems") OrderedListState<MainT> mainItems,
        @StateId("rhsItems") OrderedListState<AgainstT> rhsItems,
        @TimerFamily("joinTimers") TimerMap joinTimers,
        @TimerFamily("cleanupTimers") TimerMap cleanupTimers) {
      RawUnionValue value = element.getValue();
      Instant bucketStart = bucketTimestamp(ts);
      Instant bucketEnd = bucketStart.plus(TIMER_BUCKET);
      switch (value.getUnionTag()) {
        case MAIN_TAG:
          mainItems.add(TimestampedValue.of((MainT) value.getValue(), ts));
          joinTimers
              .get(Long.toString(bucketEnd.getMillis()))
              .withOutputTimestamp(bucketStart)
              .set(bucketEnd);
          break;
        case AGAINST_TAG:
          rhsItems.add(TimestampedValue.of((AgainstT) value.getValue(), ts));
          if (maximumJoinPeriod != null) {
            // TODO(reuvenlax): Add allowedLateness once supported.
            Instant cleanupTime = bucketEnd.plus(maximumJoinPeriod);
            cleanupTimers.get(Long.toString(bucketEnd.getMillis())).set(cleanupTime);
          }
          // TODO(reuvenlax): Once allowedLateness is supported, we might need to reset the timer.
          // Idea of how this might be implemented:
          // store a sparse histogram mainVaue item timestamps in state. Also store the maximum
          // timer firing timestamp
          // in state. If a rhs item shows up with a timestamp < the maximum timer firing, then
          // it is late. In this
          // case walk across the histogram buckets that are > the rhs timestamp, and set those
          // timers to fire again.
          // We would also have to move garbage collection of the main items out of the regular
          // timer and into a separate
          // TimerMap, which fires only after bucket-end + allowed lateness.
          // For performance, we skip all the above state if allowedLateness == 0.
          break;
        default:
          throw new RuntimeException("Unexpected union tag " + value.getUnionTag());
      }
    }

    @OnTimerFamily("joinTimers")
    public void onJoinTimer(
        @Key K key,
        @Timestamp Instant ts,
        @StateId("mainItems") OrderedListState<MainT> mainItems,
        @StateId("rhsItems") OrderedListState<AgainstT> rhsItems,
        OutputReceiver<KV<K, Result<MainT, AgainstT>>> o) {
      Instant bucketStart = ts.minus(TIMER_BUCKET);
      mainItems.readRangeLater(bucketStart, ts);

      Instant rhsFetchStart =
          (within == null)
              ? BoundedWindow.TIMESTAMP_MIN_VALUE
              : bucketStart.minus(asOfOffset).minus(within);
      Instant rhsFetchLimit = ts.minus(asOfOffset);
      rhsItems.readRangeLater(rhsFetchStart, rhsFetchLimit);
      Iterable<TimestampedValue<AgainstT>> rhsItemsReady =
          rhsItems.readRange(rhsFetchStart, rhsFetchLimit);
      Iterable<TimestampedValue<MainT>> mainItemsReady =
          mainItems.readRange(BoundedWindow.TIMESTAMP_MIN_VALUE, ts);

      PeekingIterator<TimestampedValue<AgainstT>> rhsItemIter =
          Iterators.peekingIterator(rhsItemsReady.iterator());
      TimestampedValue<AgainstT> currentAgainstItem = null;
      List<TimestampedValue<AgainstT>> currentAgainstItems = Lists.newArrayList();
      for (TimestampedValue<MainT> mainItem : mainItemsReady) {
        Instant mainTs = mainItem.getTimestamp();
        Instant asOfTimestamp = mainTs.minus(asOfOffset);
        if (this.within == null) {
          // This is an AS OF query. We need to find the the latest rhsItem with timestamps <=
          // asOfTimestamp.
          while (rhsItemIter.hasNext()) {
            if (rhsItemIter.peek().getTimestamp().isAfter(asOfTimestamp)) {
              break;
            }
            currentAgainstItem = rhsItemIter.next();
            if (!currentAgainstItems.isEmpty()
                && !currentAgainstItems
                    .get(0)
                    .getTimestamp()
                    .equals(currentAgainstItem.getTimestamp())) {
              currentAgainstItems.clear();
            }
            currentAgainstItems.add(currentAgainstItem);
          }
        } else {
          // This is a WITHIN query. We need to find all rhsItems withing [asOfTimestamp -
          // within, asOfTimestamp].
          Instant startTimestamp = asOfTimestamp.minus(within);
          while (rhsItemIter.hasNext()) {
            if (rhsItemIter.peek().getTimestamp().isBefore(startTimestamp)) {
              continue;
            } else if (rhsItemIter.peek().getTimestamp().isAfter(asOfTimestamp)) {
              break;
            }
            currentAgainstItems.add(rhsItemIter.next());
          }
        }
        boolean shouldOutputResult = outputUnjoinedElements || !currentAgainstItems.isEmpty();
        if (shouldOutputResult) {
          o.output(KV.of(key, Result.of(mainItem, currentAgainstItems)));
        }
      }
      mainItems.clearRange(GlobalWindow.TIMESTAMP_MIN_VALUE, ts);

      if (within == null) {
        // The last item in rhsItems that has a timestamp <= t must not be removed, as it may
        // still join against a future item. This means we must find the last item in the fetched
        // list
        // and make sure we leave it in there.
        if (rhsItemIter.hasNext()) {
          currentAgainstItem = Iterators.getLast(rhsItemIter);
        }
        if (currentAgainstItem != null) {
          rhsItems.clearRange(BoundedWindow.TIMESTAMP_MIN_VALUE, currentAgainstItem.getTimestamp());
        }
      } else {
        // We assume that all future timers will be > t. Since this is a within query, we can clear
        // all elements
        // within that range.
        rhsItems.clearRange(BoundedWindow.TIMESTAMP_MIN_VALUE, rhsFetchStart);
      }
    }

    @OnTimerFamily("CleanupTimers")
    public void onCleanupTimer(
        @TimerId String timerId, @StateId("rhsItems") OrderedListState<AgainstT> rhsItems) {
      Instant bucketEnd = Instant.ofEpochMilli(Long.valueOf(timerId));
      Instant bucketStart = bucketEnd.minus(TIMER_BUCKET);
      rhsItems.clearRange(bucketStart, bucketEnd);
    }
  }

  private static <K, V> Coder<K> getKeyCoder(Coder<?> entryCoder) {
    if (!(entryCoder instanceof KvCoder<?, ?>)) {
      throw new IllegalArgumentException("PCollection does not use a KvCoder");
    }
    @SuppressWarnings("unchecked")
    KvCoder<K, V> coder = (KvCoder<K, V>) entryCoder;
    return coder.getKeyCoder();
  }

  private static <K, V> Coder<V> getValueCoder(Coder<?> entryCoder) {
    if (!(entryCoder instanceof KvCoder<?, ?>)) {
      throw new IllegalArgumentException("PCollection does not use a KvCoder");
    }
    @SuppressWarnings("unchecked")
    KvCoder<K, V> coder = (KvCoder<K, V>) entryCoder;
    return coder.getValueCoder();
  }

  private static <K, V> PCollection<KV<K, RawUnionValue>> makeUnionTable(
      final int index,
      PCollection<KV<K, V>> pCollection,
      KvCoder<K, RawUnionValue> unionTableEncoder) {
    return pCollection
        .apply(
            "MakeUnionTable" + index,
            MapElements.into(unionTableEncoder.getEncodedTypeDescriptor())
                .via(kv -> KV.of(kv.getKey(), new RawUnionValue(index, kv.getValue()))))
        .setCoder(unionTableEncoder);
  }
}
