/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess.util;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.Bundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.util.InMemoryWatermarkManager.TransformWatermarks;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.common.collect.ImmutableList;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link InMemoryWatermarkManager}.
 */
@RunWith(JUnit4.class)
public class InMemoryWatermarkManagerTest implements Serializable {
  private PCollection<Integer> createdInts;

  private PCollection<Integer> filtered;
  private PCollection<KV<String, Integer>> keyed;

  private PCollection<Integer> intsToFlatten;
  private PCollection<Integer> flattened;

  private InMemoryWatermarkManager manager;

  @Before
  public void setup() {
    TestPipeline p = TestPipeline.create();

    createdInts = p.apply("createdInts", Create.of(1, 2, 3));

    filtered = createdInts.apply("filtered", Filter.greaterThan(1));
    keyed = createdInts.apply("keyed", WithKeys.<String, Integer>of("MyKey"));

    intsToFlatten = p.apply("intsToFlatten", Create.of(-1, 256, 65535));
    PCollectionList<Integer> preFlatten = PCollectionList.of(createdInts).and(intsToFlatten);
    flattened = preFlatten.apply("flattened", Flatten.<Integer>pCollections());

    Collection<AppliedPTransform<?, ?, ?>> rootTransforms =
        ImmutableList.<AppliedPTransform<?, ?, ?>>of(
            createdInts.getProducingTransformInternal(),
            intsToFlatten.getProducingTransformInternal());

    Map<PCollection<?>, Collection<AppliedPTransform<?, ?, ?>>> consumers = new HashMap<>();
    consumers.put(
        createdInts,
        ImmutableList.<AppliedPTransform<?, ?, ?>>of(filtered.getProducingTransformInternal(),
            keyed.getProducingTransformInternal(), flattened.getProducingTransformInternal()));
    consumers.put(filtered, Collections.<AppliedPTransform<?, ?, ?>>emptyList());
    consumers.put(keyed, Collections.<AppliedPTransform<?, ?, ?>>emptyList());

    consumers.put(
        intsToFlatten,
        Collections.<AppliedPTransform<?, ?, ?>>singleton(
            flattened.getProducingTransformInternal()));
    consumers.put(flattened, Collections.<AppliedPTransform<?, ?, ?>>emptyList());

    manager = InMemoryWatermarkManager.create(rootTransforms, consumers);
  }


  /**
   * Demonstrates that getWatermark, when called on an {@link AppliedPTransform} that has not
   * processed any elements, returns the {@link BoundedWindow#TIMESTAMP_MIN_VALUE}.
   */
  @Test
  public void getWatermarkForUntouchedTransform() {
    TransformWatermarks watermarks =
        manager.getWatermarks(createdInts.getProducingTransformInternal());

    assertThat(watermarks.getInputWatermark(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
    assertThat(watermarks.getOutputWatermark(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
  }

  /**
   * Demonstrates that getWatermark for a transform that consumes no input uses the Watermark
   * Hold value provided to it as the output watermark.
   */
  @Test
  public void getWatermarkForUpdatedSourceTransform() {
    Bundle<Integer> output = globallyWindowedBundle(createdInts, 1);

    TransformWatermarks updatedSourceWatermark =
        manager.updateOutputWatermark(createdInts.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(output), new Instant(8000L));

    assertThat(updatedSourceWatermark.getOutputWatermark(), equalTo(new Instant(8000L)));
  }

  /**
   * Demonstrates that getWatermark for a transform that takes multiple inputs is held to the
   * minimum watermark across all of its inputs.
   */
  @Test
  public void getWatermarkForMultiInputTransform() {
    Bundle<Integer> secondPcollectionBundle = globallyWindowedBundle(intsToFlatten, -1);

    manager.updateOutputWatermark(intsToFlatten.getProducingTransformInternal(),
        Collections.<Bundle<?>>singleton(secondPcollectionBundle), new Instant(Long.MAX_VALUE));

    // We didn't do anything for the first source, so we shouldn't have progressed the watermark
    TransformWatermarks firstSourceWatermark =
        manager.getWatermarks(createdInts.getProducingTransformInternal());
    assertThat(
        firstSourceWatermark.getOutputWatermark(),
        not(laterThan(BoundedWindow.TIMESTAMP_MIN_VALUE)));

    // the Second Source output all of the elements so it should be done (with a watermark at the
    // end of time).
    TransformWatermarks secondSourceWatermark =
        manager.getWatermarks(intsToFlatten.getProducingTransformInternal());
    assertThat(
        secondSourceWatermark.getOutputWatermark(),
        not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));

    // We haven't consumed anything yet, so our watermark should be at the beginning of time
    TransformWatermarks transformWatermark =
        manager.getWatermarks(flattened.getProducingTransformInternal());
    assertThat(
        transformWatermark.getInputWatermark(), not(laterThan(BoundedWindow.TIMESTAMP_MIN_VALUE)));
    assertThat(
        transformWatermark.getOutputWatermark(), not(laterThan(BoundedWindow.TIMESTAMP_MIN_VALUE)));

    Bundle<Integer> flattenedBundleSecondCreate = globallyWindowedBundle(flattened, -1);
    // We have finished processing the bundle from the second PCollection, but we haven't consumed
    // anything from the first PCollection yet; so our watermark shouldn't advance
    TransformWatermarks transformAfterProcessing =
        manager.updateWatermarks(secondPcollectionBundle, flattened.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(flattenedBundleSecondCreate), null);
    assertThat(
        transformAfterProcessing.getInputWatermark(),
        not(laterThan(BoundedWindow.TIMESTAMP_MIN_VALUE)));
    assertThat(
        transformAfterProcessing.getOutputWatermark(),
        not(laterThan(BoundedWindow.TIMESTAMP_MIN_VALUE)));

    Instant firstCollectionTimestamp = new Instant(10000);
    Bundle<Integer> firstPcollectionBundle = timestampedBundle(
        createdInts, TimestampedValue.<Integer>of(5, firstCollectionTimestamp));
    // the source is done, but elements are still buffered. The source output watermark should be
    // past the end of the global window
    TransformWatermarks firstSourceWatermarks =
        manager.updateOutputWatermark(createdInts.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(firstPcollectionBundle), new Instant(Long.MAX_VALUE));
    assertThat(firstSourceWatermarks.getOutputWatermark(),
        not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));

    // We still haven't consumed any of the first source's input, so the watermark should still not
    // progress
    TransformWatermarks flattenAfterSourcesProduced =
        manager.getWatermarks(flattened.getProducingTransformInternal());
    assertThat(
        flattenAfterSourcesProduced.getInputWatermark(),
        not(laterThan(firstCollectionTimestamp)));
    assertThat(
        flattenAfterSourcesProduced.getOutputWatermark(),
        not(laterThan(firstCollectionTimestamp)));

    // We have buffered inputs, but since the PCollection has all of the elements (has a WM past the
    // end of the global window), we should have a watermark equal to the min among buffered
    // elements
    TransformWatermarks withBufferedElements =
        manager.getWatermarks(flattened.getProducingTransformInternal());
    assertThat(withBufferedElements.getInputWatermark(), equalTo(firstCollectionTimestamp));
    assertThat(withBufferedElements.getOutputWatermark(), equalTo(firstCollectionTimestamp));

    Bundle<?> completedFlattenBundle = InProcessBundle.unkeyed(flattened);
    TransformWatermarks afterConsumingAllInput =
        manager.updateWatermarks(firstPcollectionBundle, flattened.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(completedFlattenBundle), null);
    assertThat(
        afterConsumingAllInput.getInputWatermark(),
        not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));
    assertThat(
        afterConsumingAllInput.getOutputWatermark(),
        not(laterThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));
  }

  /**
   * Demonstrates that pending elements are independent among
   * {@link AppliedPTransform AppliedPTransforms} that consume the same input {@link PCollection}.
   */
  @Test
  public void getWatermarkForMultiConsumedCollection() {
    Bundle<Integer> createdBundle = timestampedBundle(createdInts,
        TimestampedValue.of(1, new Instant(1_000_000L)), TimestampedValue.of(2, new Instant(1234L)),
        TimestampedValue.of(3, new Instant(-1000L)));
    TransformWatermarks createdAfterProducing =
        manager.updateOutputWatermark(createdInts.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(createdBundle), new Instant(Long.MAX_VALUE));
    assertThat(createdAfterProducing.getOutputWatermark(),
        not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));

    Bundle<KV<String, Integer>> keyBundle =
        timestampedBundle(keyed, TimestampedValue.of(KV.of("MyKey", 1), new Instant(1_000_000L)),
            TimestampedValue.of(KV.of("MyKey", 2), new Instant(1234L)),
            TimestampedValue.of(KV.of("MyKey", 3), new Instant(-1000L)));
    TransformWatermarks keyedWatermarks = manager.updateWatermarks(createdBundle,
        keyed.getProducingTransformInternal(), Collections.<Bundle<?>>singleton(keyBundle), null);
    assertThat(
        keyedWatermarks.getInputWatermark(), not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));
    assertThat(
        keyedWatermarks.getOutputWatermark(), not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));

    TransformWatermarks filteredWatermarks =
        manager.getWatermarks(filtered.getProducingTransformInternal());
    assertThat(filteredWatermarks.getInputWatermark(), not(laterThan(new Instant(-1000L))));
    assertThat(
        filteredWatermarks.getOutputWatermark(), not(laterThan(new Instant(-1000L))));

    Bundle<Integer> filteredBundle = timestampedBundle(
        filtered, TimestampedValue.of(2, new Instant(1234L)));
    TransformWatermarks filteredProcessedWatermarks =
        manager.updateWatermarks(createdBundle, filtered.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(filteredBundle), null);
    assertThat(
        filteredProcessedWatermarks.getInputWatermark(),
        not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));
    assertThat(
        filteredProcessedWatermarks.getOutputWatermark(),
        not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));
  }

  /**
   * Demonstrates that the watermark of an {@link AppliedPTransform} is held to the provided
   * watermark hold.
   */
  @Test
  public void updateWatermarkWithWatermarkHolds() {
    Bundle<Integer> createdBundle = timestampedBundle(createdInts,
        TimestampedValue.of(1, new Instant(1_000_000L)),
        TimestampedValue.of(2, new Instant(1234L)),
        TimestampedValue.of(3, new Instant(-1000L)));
    manager.updateOutputWatermark(createdInts.getProducingTransformInternal(),
        Collections.<Bundle<?>>singleton(createdBundle), new Instant(Long.MAX_VALUE));

    Bundle<KV<String, Integer>> keyBundle = timestampedBundle(
        keyed, TimestampedValue.of(KV.of("MyKey", 1), new Instant(1_000_000L)),
        TimestampedValue.of(KV.of("MyKey", 2), new Instant(1234L)),
        TimestampedValue.of(KV.of("MyKey", 3), new Instant(-1000L)));
    TransformWatermarks keyedWatermarks = manager.updateWatermarks(createdBundle,
        keyed.getProducingTransformInternal(), Collections.<Bundle<?>>singleton(keyBundle),
        new Instant(500L));
    assertThat(
        keyedWatermarks.getInputWatermark(), not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));
    assertThat(keyedWatermarks.getOutputWatermark(), not(laterThan(new Instant(500L))));
  }

  /**
   * Demonstrates that updated output watermarks are monotonic in the presence of late data, when
   * called on an {@link AppliedPTransform} that consumes no input.
   */
  @Test
  public void updateOutputWatermarkShouldBeMonotonic() {
    Bundle<?> firstInput = InProcessBundle.unkeyed(createdInts);
    TransformWatermarks firstWatermarks =
        manager.updateOutputWatermark(createdInts.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(firstInput), new Instant(0L));
    assertThat(firstWatermarks.getOutputWatermark(), equalTo(new Instant(0L)));

    Bundle<?> secondInput = InProcessBundle.unkeyed(createdInts);
    TransformWatermarks secondWatermarks =
        manager.updateOutputWatermark(createdInts.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(secondInput), new Instant(-250L));
    assertThat(secondWatermarks.getOutputWatermark(), not(earlierThan(new Instant(0L))));
  }

  /**
   * Demonstrates that updated output watermarks are monotonic in the presence of watermark holds
   * that become earlier than a previous watermark hold.
   */
  @Test
  public void updateWatermarkWithHoldsShouldBeMonotonic() {
    Bundle<Integer> createdBundle = timestampedBundle(
        createdInts,
        TimestampedValue.of(1, new Instant(1_000_000L)),
        TimestampedValue.of(2, new Instant(1234L)),
        TimestampedValue.of(3, new Instant(-1000L)));
    manager.updateOutputWatermark(
        createdInts.getProducingTransformInternal(),
        Collections.<Bundle<?>>singleton(createdBundle),
        new Instant(Long.MAX_VALUE));

    Bundle<KV<String, Integer>> keyBundle = timestampedBundle(
            keyed,
            TimestampedValue.of(KV.of("MyKey", 1), new Instant(1_000_000L)),
            TimestampedValue.of(KV.of("MyKey", 2), new Instant(1234L)),
            TimestampedValue.of(KV.of("MyKey", 3), new Instant(-1000L)));
    TransformWatermarks keyedWatermarks =
        manager.updateWatermarks(createdBundle, keyed.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(keyBundle), new Instant(500L));
    assertThat(
        keyedWatermarks.getInputWatermark(), not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));
    assertThat(keyedWatermarks.getOutputWatermark(), not(laterThan(new Instant(500L))));
    Instant oldOutputWatermark = keyedWatermarks.getOutputWatermark();

    TransformWatermarks updatedWatermarks =
        manager.getWatermarks(keyed.getProducingTransformInternal());
    assertThat(
        updatedWatermarks.getInputWatermark(), not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));
    // We added a hold prior to the old watermark; we shouldn't progress (due to the earlier hold)
    // but the watermark is monotonic and should not backslide to the new, earlier hold
    assertThat(
        updatedWatermarks.getOutputWatermark(), equalTo(oldOutputWatermark));
  }

  /**
   * Demonstrates that updateWatermarks in the presence of late data is monotonic.
   */
  @Test
  public void updateWatermarkWithLateData() {
    Instant sourceWatermark = new Instant(1_000_000L);
    Bundle<Integer> createdBundle = timestampedBundle(createdInts,
        TimestampedValue.of(1, sourceWatermark), TimestampedValue.of(2, new Instant(1234L)));

    manager.updateOutputWatermark(createdInts.getProducingTransformInternal(),
        Collections.<Bundle<?>>singleton(createdBundle), sourceWatermark);

    Bundle<KV<String, Integer>> keyBundle = timestampedBundle(
        keyed,
        TimestampedValue.of(KV.of("MyKey", 1), sourceWatermark),
        TimestampedValue.of(KV.of("MyKey", 2), new Instant(1234L)));

    // Finish processing the on-time data. The watermarks should progress to be equal to the source
    TransformWatermarks onTimeWatermarks =
        manager.updateWatermarks(createdBundle, keyed.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(keyBundle), null);
    assertThat(onTimeWatermarks.getInputWatermark(), equalTo(sourceWatermark));
    assertThat(onTimeWatermarks.getOutputWatermark(), equalTo(sourceWatermark));

    Bundle<Integer> lateDataBundle = timestampedBundle(
        createdInts, TimestampedValue.of(3, new Instant(-1000L)));

    // the late data arrives in a downstream PCollection after its watermark has advanced past it;
    // we don't advance the watermark past the current watermark until we've consumed the late data
    TransformWatermarks bufferedLateWm =
        manager.updateOutputWatermark(createdInts.getProducingTransformInternal(),
           Collections.<Bundle<?>>singleton(lateDataBundle),
            new Instant(2_000_000L));
    assertThat(bufferedLateWm.getOutputWatermark(), equalTo(new Instant(2_000_000L)));

    // The input watermark should be held to its previous value (not advanced due to late data; not
    // moved backwards in the presence of watermarks due to monotonicity).
    TransformWatermarks lateDataBufferedWatermark =
        manager.getWatermarks(keyed.getProducingTransformInternal());
    assertThat(lateDataBufferedWatermark.getInputWatermark(), not(earlierThan(sourceWatermark)));
    assertThat(lateDataBufferedWatermark.getOutputWatermark(), not(earlierThan(sourceWatermark)));

    Bundle<KV<String, Integer>> lateKeyedBundle = timestampedBundle(
        keyed, TimestampedValue.of(KV.of("MyKey", 3), new Instant(-1000L)));
    manager.updateWatermarks(lateDataBundle, keyed.getProducingTransformInternal(),
        Collections.<Bundle<?>>singleton(lateKeyedBundle), null);
  }

  /**
   * Demonstrates that after watermarks of an upstream transform are updated, but no output has been
   * produced, the watermarks of a downstream process are advanced.
   */
  @Test
  public void getWatermarksAfterOnlyEmptyOutput() {
    Bundle<Integer> emptyCreateOutput = globallyWindowedBundle(createdInts);
    TransformWatermarks updatedSourceWatermarks =
        manager.updateOutputWatermark(createdInts.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(emptyCreateOutput), BoundedWindow.TIMESTAMP_MAX_VALUE);

    assertThat(updatedSourceWatermarks.getOutputWatermark(),
        not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));

    TransformWatermarks finishedFilterWatermarks =
        manager.getWatermarks(filtered.getProducingTransformInternal());
    assertThat(finishedFilterWatermarks.getInputWatermark(),
        not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));
    assertThat(finishedFilterWatermarks.getOutputWatermark(),
        not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));
  }

  /**
   * Demonstrates that after watermarks of an upstream transform are updated, but no output has been
   * produced, and the downstream transform has a watermark hold, the watermark is held to the hold.
   */
  @Test
  public void getWatermarksAfterHoldAndEmptyOutput() {
    Bundle<Integer> firstCreateOutput = globallyWindowedBundle(createdInts, 1, 2);
    manager.updateOutputWatermark(createdInts.getProducingTransformInternal(),
        Collections.<Bundle<?>>singleton(firstCreateOutput), new Instant(12_000L));

    Bundle<Integer> firstFilterOutput = globallyWindowedBundle(filtered);
    TransformWatermarks firstFilterWatermarks =
        manager.updateWatermarks(firstCreateOutput, filtered.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(firstFilterOutput), new Instant(10_000L));
    assertThat(firstFilterWatermarks.getInputWatermark(), not(earlierThan(new Instant(12_000L))));
    assertThat(firstFilterWatermarks.getOutputWatermark(), not(laterThan(new Instant(10_000L))));

    Bundle<Integer> emptyCreateOutput = globallyWindowedBundle(createdInts);
    TransformWatermarks updatedSourceWatermarks =
        manager.updateOutputWatermark(createdInts.getProducingTransformInternal(),
            Collections.<Bundle<?>>singleton(emptyCreateOutput), BoundedWindow.TIMESTAMP_MAX_VALUE);

    assertThat(updatedSourceWatermarks.getOutputWatermark(),
        not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));

    TransformWatermarks finishedFilterWatermarks =
        manager.getWatermarks(filtered.getProducingTransformInternal());
    assertThat(finishedFilterWatermarks.getInputWatermark(),
        not(earlierThan(BoundedWindow.TIMESTAMP_MAX_VALUE)));
    assertThat(finishedFilterWatermarks.getOutputWatermark(), not(laterThan(new Instant(10_000L))));
  }

  private static Matcher<Instant> earlierThan(final Instant laterInstant) {
    return new BaseMatcher<Instant>() {
      @Override
      public boolean matches(Object item) {
        ReadableInstant instant = (ReadableInstant) item;
        return instant.isBefore(laterInstant);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("earlier than ").appendValue(laterInstant);
      }
    };
  }

  private static Matcher<Instant> laterThan(final Instant shouldBeEarlier) {
    return new BaseMatcher<Instant>() {
      @Override
      public boolean matches(Object item) {
        ReadableInstant instant = (ReadableInstant) item;
        return instant.isAfter(shouldBeEarlier);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("later than ").appendValue(shouldBeEarlier);
      }};
  }

  @SafeVarargs
  private final <T> Bundle<T> timestampedBundle(
      PCollection<T> pc, TimestampedValue<T>... values) {
    Bundle<T> bundle = InProcessBundle.unkeyed(pc);
    for (TimestampedValue<T> value : values) {
      bundle.add(
          WindowedValue.timestampedValueInGlobalWindow(value.getValue(), value.getTimestamp()));
    }
    return bundle;
  }

  @SafeVarargs
  private final <T> Bundle<T> globallyWindowedBundle(PCollection<T> pc, T... values) {
    Bundle<T> bundle = InProcessBundle.unkeyed(pc);
    for (T value : values) {
      bundle.add(WindowedValue.valueInGlobalWindow(value));
    }
    return bundle;
  }
}
