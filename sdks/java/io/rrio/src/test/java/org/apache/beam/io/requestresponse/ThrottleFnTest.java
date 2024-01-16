package org.apache.beam.io.requestresponse;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnOutputReceivers;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/** Tests for {@link ThrottleWithoutExternalResource.ThrottleFn}. */
@RunWith(JUnit4.class)
public class ThrottleFnTest {
   @Test
   public void givenKVValueNull_thenGetInitialRange_isEmpty() {
       ThrottleWithoutExternalResource.ThrottleFn<Integer> fn = throttleFn();
       ThrottleWithoutExternalResource.OffsetRange restriction = fn.getInitialRange(KV.of(0, null));
       assertThat(restriction.hasMoreOffset(), is(false));
   }

   @Test
   public void givenKVValueNull_thenProcess_emitsNothing() {
       TestOutputReceiver receiver = new TestOutputReceiver();
       ThrottleWithoutExternalResource.ThrottleFn<Integer> fn = throttleFn();
       ThrottleWithoutExternalResource.OffsetRange restriction = fn.getInitialRange(KV.of(0, null));
       fn.process(KV.of(0, null), new WatermarkEstimators.Manual(Instant.now()), restriction.newTracker(), receiver);
       assertThat(receiver.getOutputMap().isEmpty(), is(true));
   }

   @Test
    public void givenKVValueEmpty_thenGetInitialRange_isEmpty() {
       ThrottleWithoutExternalResource.ThrottleFn<Integer> fn = throttleFn();
       ThrottleWithoutExternalResource.OffsetRange restriction = fn.getInitialRange(KV.of(0, Collections.emptyList()));
       assertThat(restriction.hasMoreOffset(), is(false));
   }

   @Test
   public void givenKVValueEmpty_thenProcess_emitsNothing() {
       TestOutputReceiver receiver = new TestOutputReceiver();
       ThrottleWithoutExternalResource.ThrottleFn<Integer> fn = throttleFn();
       ThrottleWithoutExternalResource.OffsetRange restriction = fn.getInitialRange(KV.of(0, Collections.emptyList()));
       fn.process(KV.of(0, null), new WatermarkEstimators.Manual(Instant.now()), restriction.newTracker(), receiver);
       assertThat(receiver.getOutputMap().isEmpty(), is(true));
   }

   @Test
    public void givenKVValueSparse_thenGetInitialRange_isNotEmpty() {
       ThrottleWithoutExternalResource.ThrottleFn<Integer> fn = throttleFn();
       ThrottleWithoutExternalResource.OffsetRange restriction = fn.getInitialRange(KV.of(0, Arrays.asList(1, 2, 3)));
       assertThat(restriction.hasMoreOffset(), is(true));
       assertThat(restriction.getToExclusive(), is(3));
       ThrottleWithoutExternalResource.OffsetRangeTracker tracker = restriction.newTracker();
       assertThat(tracker.tryClaim(0), is(true));
       assertThat(tracker.tryClaim(1), is(true));
       assertThat(tracker.tryClaim(2), is(false));
   }

   @Test
   public void givenKVValueSparse_thenProcess_emitsImmediately() {
       TestOutputReceiver receiver = new TestOutputReceiver();
       KV<Integer, List<Integer>> element = KV.of(0, Arrays.asList(1, 2, 3));
       ThrottleWithoutExternalResource.ThrottleFn<Integer> fn = throttleFn();
       ThrottleWithoutExternalResource.OffsetRange restriction = fn.getInitialRange(element);
       ManualWatermarkEstimator<Instant> watermarkEstimator = (ManualWatermarkEstimator<Instant>) fn.newWatermarkEstimator(Instant.now());
       fn.process(element, watermarkEstimator, restriction.newTracker(), receiver);
       assertThat(receiver.getOutputMap().values().isEmpty(), is(false));
       List<List<Integer>> lists = new ArrayList<>(receiver.getOutputMap().values());
       assertThat(lists.size(), is(3));
   }

   private static ThrottleWithoutExternalResource.ThrottleFn<Integer> throttleFn() {
       return new ThrottleWithoutExternalResource.ThrottleFn<>(configuration());
   }

   private static ThrottleWithoutExternalResource.Configuration configuration() {
       return ThrottleWithoutExternalResource.Configuration.builder()
               .setMaximumRate(Rate.of(1, Duration.millis(1L)))
               .build();
   }

   private static class TestOutputReceiver implements DoFn.OutputReceiver<Integer> {

       private final Map<Instant, List<Integer>> outputMap = new HashMap<>();

       @Override
       public void output(Integer output) {

       }

       @Override
       public void outputWithTimestamp(Integer output, Instant timestamp) {
           if (!outputMap.containsKey(timestamp)) {
               outputMap.put(timestamp, new ArrayList<>());
           }
           outputMap.get(timestamp).add(output);
       }

       public Map<Instant, List<Integer>> getOutputMap() {
           return outputMap;
       }

       @Override
       public void outputWindowedValue(Integer output, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo paneInfo) {

       }
   }
}
