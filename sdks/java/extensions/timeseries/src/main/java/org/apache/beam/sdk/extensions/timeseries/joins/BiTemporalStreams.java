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
package org.apache.beam.sdk.extensions.timeseries.joins;

import java.util.*;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This sample will take two streams left stream and right stream. It will match the left stream to
 * the nearest right stream based on their timestamp. Nearest left stream is either <= to the right
 * stream timestamp.
 *
 * <p>If two values in the right steam have the same timestamp then the results are
 * non-deterministic.
 */
@Experimental
public class BiTemporalStreams {

  public static <K, V1, V2> BiTemporalJoin<K, V1, V2> join(
      TupleTag<V1> leftTag, TupleTag<V2> rightTag, Duration window) {
    return new BiTemporalJoin<K, V1, V2>(leftTag, rightTag, window);
  }

  @Experimental
  public static class BiTemporalJoin<K, V1, V2>
      extends PTransform<KeyedPCollectionTuple<K>, PCollection<BiTemporalJoinResult<K, V1, V2>>> {

    private static final Logger LOG = LoggerFactory.getLogger(BiTemporalStreams.class);

    // Sets the limit at which point the processed left stream values are garbage collected
    static int GC_LIMIT = 1000;

    Coder<V1> leftCoder;
    Coder<V2> rightCoder;

    TupleTag<V1> leftTag;
    TupleTag<V2> rightTag;

    Duration window;

    public BiTemporalJoin(TupleTag<V1> leftTag, TupleTag<V2> rightTag, Duration window) {
      this.leftTag = leftTag;
      this.rightTag = rightTag;
      this.window = window;
    }

    public static <K, V1, V2> BiTemporalJoin create(
        TupleTag<V1> leftTag, TupleTag<V2> rightTag, Duration window) {
      return new BiTemporalJoin<K, V1, V2>(leftTag, rightTag, window);
    }

    public BiTemporalJoin setGCLimit(int gcLimit) {
      GC_LIMIT = gcLimit;
      return this;
    }

    @Override
    public PCollection<BiTemporalJoinResult<K, V1, V2>> expand(KeyedPCollectionTuple<K> input) {

      List<KeyedPCollectionTuple.TaggedKeyedPCollection<K, ?>> collections =
          input.getKeyedCollections();

      PCollection<KV<K, V1>> leftCollection = null;
      PCollection<KV<K, V2>> rightCollection = null;

      for (KeyedPCollectionTuple.TaggedKeyedPCollection<K, ?> t : collections) {

        if (t.getTupleTag().equals(leftTag)) {
          leftCollection =
              ((KeyedPCollectionTuple.TaggedKeyedPCollection<K, V1>) t).getCollection();
        }

        if (t.getTupleTag().equals(rightTag)) {
          rightCollection =
              ((KeyedPCollectionTuple.TaggedKeyedPCollection<K, V2>) t).getCollection();
        }
      }

      leftCoder = ((KvCoder<K, V1>) leftCollection.getCoder()).getValueCoder();
      rightCoder = ((KvCoder<K, V2>) rightCollection.getCoder()).getValueCoder();

      BiTemporalJoinResultCoder<K, V1, V2> biStreamJoinResultCoder =
          new BiTemporalJoinResultCoder(input.getKeyCoder(), leftCoder, rightCoder);

      PCollectionList<KV<K, BiTemporalJoinResult<K, V1, V2>>> l =
          PCollectionList.of(
                  leftCollection
                      .apply(ParDo.of(new CreateStreamDataLeft<K, V1, V2>()))
                      .setCoder(KvCoder.of(input.getKeyCoder(), biStreamJoinResultCoder)))
              .and(
                  rightCollection
                      .apply(ParDo.of(new CreateStreamDataRight<K, V1, V2>()))
                      .setCoder(KvCoder.of(input.getKeyCoder(), biStreamJoinResultCoder)));

      return l.apply(Flatten.pCollections())
          .apply(Window.into(FixedWindows.of(window)))
          .apply(
              ParDo.of(new StreamMatcher<K, V1, V2>(input.getKeyCoder(), leftCoder, rightCoder)));
    }

    public static class CreateStreamDataLeft<K, V1, V2>
        extends DoFn<KV<K, V1>, KV<K, BiTemporalJoinResult<K, V1, V2>>> {

      public static <K, V1, V2> CreateStreamDataLeft create() {
        return new CreateStreamDataLeft<K, V1, V2>();
      }

      @ProcessElement
      public void process(
          ProcessContext c, OutputReceiver<KV<K, BiTemporalJoinResult<K, V1, V2>>> o) {

        o.output(
            KV.of(
                c.element().getKey(),
                new BiTemporalJoinResult().setLeftData(c.element(), c.timestamp())));
      }
    }

    public static class CreateStreamDataRight<K, V1, V2>
        extends DoFn<KV<K, V2>, KV<K, BiTemporalJoinResult<K, V1, V2>>> {

      public static <K, V1, V2> CreateStreamDataRight create() {
        return new CreateStreamDataRight<K, V1, V2>();
      }

      @ProcessElement
      public void process(
          ProcessContext c, OutputReceiver<KV<K, BiTemporalJoinResult<K, V1, V2>>> o) {
        o.output(
            KV.of(
                c.element().getKey(),
                new BiTemporalJoinResult().setRightData(c.element(), c.timestamp())));
      }
    }

    public static class StreamMatcher<K, V1, V2>
        extends DoFn<KV<K, BiTemporalJoinResult<K, V1, V2>>, BiTemporalJoinResult<K, V1, V2>> {

      // State used to hold all right stream values
      @StateId("rightStream")
      private final StateSpec<BagState<TimestampedValue<V2>>> rightStream;
      // State used to hold all left stream values
      @StateId("leftStream")
      private final StateSpec<BagState<TimestampedValue<V1>>> leftStream;
      // The timestamp of the existing processing timer Null if not set
      @StateId("timerTimestamp")
      private final StateSpec<ValueState<Long>> timerTimestamp =
          StateSpecs.value(VarLongCoder.of());
      // The key, which is required to create a unique window-key for optimization cache
      @StateId("dataKey")
      private final StateSpec<ValueState<K>> dataKey;
      // The process timer used to match left stream values to right stream values
      @TimerId("processTimer")
      private final TimerSpec processTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

      // ----------- State Variables
      // Completed work timestamp, used for the GC to indicate the work it needs to do.
      @StateId("lastProcessedTradeTimestampState")
      private final StateSpec<ValueState<Long>> lastProcessedTradeTimestampState =
          StateSpecs.value(VarLongCoder.of());
      // Bundle storage of sorted list, used to assist wih O(n^2) problem until Sorted Map is
      // available
      Map<WindowedValue, List<TimestampedValue<V1>>> leftCache;
      Map<WindowedValue, List<TimestampedValue<V2>>> rightCache;
      Coder<V1> leftCoder;
      Coder<V2> rightCoder;
      Coder<K> keyCoder;

      public StreamMatcher(Coder<K> keyCoder, Coder<V1> leftCoder, Coder<V2> rightCoder) {
        this.leftCoder = leftCoder;
        this.rightCoder = rightCoder;
        this.keyCoder = keyCoder;
        this.rightStream = StateSpecs.bag(TimestampedValue.TimestampedValueCoder.of(rightCoder));
        this.leftStream = StateSpecs.bag(TimestampedValue.TimestampedValueCoder.of(leftCoder));
        this.dataKey = StateSpecs.value(keyCoder);
      }

      public static <K, V1, V2> BiTemporalJoinResult createMatch(
          K key, List<TimestampedValue<V2>> right, TimestampedValue<V1> left, int idx) {

        BiTemporalJoinResult<K, V1, V2> m = new BiTemporalJoinResult();
        m.setLeftData(KV.of(key, left.getValue()), left.getTimestamp());

        int adjustedIdx;
        TimestampedValue<V2> value;

        // If exact match found or if the largest timestamp in the list is smaller than the right
        // streams timestamp id
        // Note if there are duplicate for the same timestamp the results will be non-deterministic
        if (idx >= 0) {
          adjustedIdx = idx;
          value = right.get(adjustedIdx);
          m.setRightData(KV.of(key, value.getValue()), value.getTimestamp());
          m.setMatched(true);
        }
        if (right.size() > 0 && idx == -1 - right.size()) {
          adjustedIdx = right.size() - 1;
          value = right.get(adjustedIdx);
          m.setRightData(KV.of(key, value.getValue()), value.getTimestamp());
          m.setMatched(true);
        }
        if (idx < -1) {
          adjustedIdx = Math.abs(idx) - 2;
          value = right.get(adjustedIdx);
          m.setRightData(KV.of(key, value.getValue()), value.getTimestamp());
          m.setMatched(true);
        }
        if (idx == -1) {
          m.setMatched(false);
        }

        return m;
      }

      /*
       Right stream values are added to the bag
       Left stream values are added to the bag
       A timer is set if no timer exists already or if min(timerTimestamp) is > current object timestamp

       If timertimestamp is NULL then a timer is set in OnProcess()
       If On OnTimer firing there are elements in the left stream where  EventTime > OnTimer.timestamp then a new timer is also set.
      */
      @ProcessElement
      public void process(
          @Element KV<K, BiTemporalJoinResult<K, V1, V2>> input,
          @Timestamp Instant timestamp,
          OutputReceiver<BiTemporalJoinResult<K, V1, V2>> o,
          @StateId("rightStream") BagState<TimestampedValue<V2>> rightStream,
          @StateId("leftStream") BagState<TimestampedValue<V1>> leftStream,
          @StateId("timerTimestamp") ValueState<Long> timerTimestamp,
          @StateId("dataKey") ValueState<K> dataKey,
          @TimerId("processTimer") Timer processTimer) {

        // This workaround is due to OnTimerContext not supporting key access
        if (dataKey.read() == null) {
          dataKey.write(input.getKey());
        }

        if (input.getValue().getRightData() != null) {
          TimestampedValue<V2> quote = input.getValue().getRightData();
          rightStream.add(quote);
        }

        if (input.getValue().getLeftData() != null) {
          TimestampedValue<V1> rightStreamValue = input.getValue().getLeftData();
          leftStream.add(rightStreamValue);

          long processTimerTimestamp = Optional.ofNullable(timerTimestamp.read()).orElse(0L);

          if (processTimerTimestamp == 0 || timestamp.getMillis() < processTimerTimestamp) {
            processTimer.set(timestamp);
            timerTimestamp.write(timestamp.getMillis());
          }
        }
      }

      @StartBundle
      public void startBundle(StartBundleContext sbc) {

        leftCache = null;
        rightCache = null;
      }

      /**
       * The processTimer checks if there are left stream values beyound the last processed date, if
       * there is then it will check for a match and output as well as increment the processed date.
       */
      @OnTimer("processTimer")
      public void OnTimer(
          OnTimerContext otc,
          @Timestamp Instant onTimerTimestamp,
          OutputReceiver<BiTemporalJoinResult<K, V1, V2>> output,
          @StateId("rightStream") BagState<TimestampedValue<V2>> rightStream,
          @StateId("leftStream") BagState<TimestampedValue<V1>> leftStream,
          @StateId("timerTimestamp") ValueState<Long> timerTimestamp,
          @StateId("dataKey") ValueState<K> dataKey,
          @StateId("lastProcessedTradeTimestampState")
              ValueState<Long> lastProcessedTradeTimestampState,
          @TimerId("processTimer") Timer processTimer) {

        // Init caches if currently null
        rightCache = Optional.ofNullable(rightCache).orElse(new HashMap<>());
        leftCache = Optional.ofNullable(leftCache).orElse(new HashMap<>());

        // Read the quote & rightStreamValue lists
        List<TimestampedValue<V2>> quoteList =
            getSortedList(otc.window(), rightStream, rightCache, dataKey.read());

        List<TimestampedValue<V1>> rightStreamValueList =
            getSortedList(otc.window(), leftStream, leftCache, dataKey.read());

        long processTimerTimestamp = onTimerTimestamp.getMillis();

        Iterator<TimestampedValue<V1>> rightStreamValueIterator = rightStreamValueList.iterator();

        long lastProcessedTradeTimestamp =
            Optional.ofNullable(lastProcessedTradeTimestampState.read()).orElse(0L);

        // Index position of rightStreamValue queue
        int prevIdx = 0;

        // Procssed Trade count
        int processedTradeCount = 0;

        TimestampedValue<V1> rightStreamValue = null;

        while (rightStreamValueIterator.hasNext()) {

          rightStreamValue = rightStreamValueIterator.next();

          // Check if there is more work left in the rightStreamValue queue
          if (rightStreamValue.getTimestamp().getMillis() > processTimerTimestamp) {
            break;
          }

          // If we greater than the processed Watermark and below or equal to our process timestamp
          // Look for a match for the rightStreamValue in right stream values and send out results.
          // This transform does not yet support late data, this will not work if late data arrives
          if (rightStreamValue.getTimestamp().getMillis() > lastProcessedTradeTimestamp) {

            if (rightStreamValue.getTimestamp().getMillis() <= processTimerTimestamp) {

              List<TimestampedValue<V2>> subList = quoteList.subList(prevIdx, quoteList.size());

              int idx =
                  Collections.binarySearch(
                      subList,
                      rightStreamValue,
                      Comparator.comparing(TimestampedValue::getTimestamp));

              BiTemporalJoinResult<K, V1, V2> m =
                  createMatch(dataKey.read(), subList, rightStreamValue, idx);

              prevIdx = prevIdx + idx;

              output.outputWithTimestamp(m, rightStreamValue.getTimestamp());
              lastProcessedTradeTimestamp = rightStreamValue.getTimestamp().getMillis();
            }
          }
          // Increase the processed Trade Counter to check for GC LIMIT
          ++processedTradeCount;
        }

        // Check if there is more work left in the rightStreamValue queue
        // Create timer to fire if there is
        // If the number of old left stream values in the queue is @ GC_LIMIT then do GC
        // If no work left delete the Trade Queue

        if (rightStreamValue != null
            && rightStreamValue.getTimestamp().getMillis() > processTimerTimestamp) {
          processTimer.set(rightStreamValue.getTimestamp());
          timerTimestamp.write(rightStreamValue.getTimestamp().getMillis());

          // Clear down queue if we are at GC_LIMIT
          if (processedTradeCount >= GC_LIMIT) {
            List<TimestampedValue<V1>> leftStreamValues =
                rightStreamValueList.subList(processedTradeCount - 1, rightStreamValueList.size());
            leftStream.clear();
            leftStreamValues.forEach(i -> leftStream.add(i));
            putSortedList(otc.window(), leftStreamValues, leftCache, dataKey.read());
            LOG.debug(
                "GC - Partial WM is {} size of queue is {} ",
                onTimerTimestamp,
                leftStreamValues.size());
          }
        } else {
          timerTimestamp.clear();
          putSortedList(otc.window(), null, leftCache, dataKey.read());
          putSortedList(otc.window(), null, rightCache, dataKey.read());
          LOG.info("GC - Full WM is {}", onTimerTimestamp);
        }

        lastProcessedTradeTimestampState.write(lastProcessedTradeTimestamp);
      }

      // Get the sorted list from our Cache
      public <V> List<TimestampedValue<V>> getSortedList(
          BoundedWindow window,
          BagState<TimestampedValue<V>> stream,
          Map<WindowedValue, List<TimestampedValue<V>>> cache,
          K elementKey) {

        // Store this window-key combination in the Cache
        WindowedValue cacheKey = getCacheKeyForWindow(elementKey, window.maxTimestamp(), window);

        List<TimestampedValue<V>> sortedBundleList =
            cache.computeIfAbsent(cacheKey, key -> createAndLoadSortedList(window, stream));

        cache.put(cacheKey, sortedBundleList);

        return sortedBundleList;
      }

      // Build the sorted list if its not in the Cache.
      public <V> List<TimestampedValue<V>> createAndLoadSortedList(
          BoundedWindow window, BagState<TimestampedValue<V>> stream) {

        List<TimestampedValue<V>> sortedBundleList = new ArrayList<>();
        final List<TimestampedValue<V>> data = sortedBundleList;
        stream.read().forEach(t -> data.add(t));
        data.sort(Comparator.comparing(t -> t.getTimestamp()));
        sortedBundleList = data;

        LOG.debug("Catch miss for maxTime {} ", window.maxTimestamp());

        return sortedBundleList;
      }

      // Put the sorted list into the cache.
      public <V> void putSortedList(
          BoundedWindow window,
          List<TimestampedValue<V>> sortedBundleList,
          Map<WindowedValue, List<TimestampedValue<V>>> cache,
          K elementKey) {

        WindowedValue cacheKey = getCacheKeyForWindow(elementKey, window.maxTimestamp(), window);

        cache.put(cacheKey, sortedBundleList);
      }

      // Create a unique key for this Window-Key combination in the cache
      public WindowedValue<K> getCacheKeyForWindow(
          K elementKey, Instant timestamp, BoundedWindow boundedWindow) {

        return WindowedValue.of(elementKey, timestamp, boundedWindow, PaneInfo.NO_FIRING);
      }
    }
  }
}
