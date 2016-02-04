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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.Bundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;

import org.joda.time.Instant;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

/**
 * Manages watermarks of {@link PCollection PCollections} and input and output watermarks of
 * {@link AppliedPTransform AppliedPTransforms} to provide event-time and completion tracking for
 * in-memory execution. {@link InMemoryWatermarkManager} is designed to update and return a
 * consistent view of watermarks in the presence of concurrent updates.
 *
 * <p>An {@link InMemoryWatermarkManager} is provided with the collection of root
 * {@link AppliedPTransform AppliedPTransforms} and a map of {@link PCollection PCollections} to
 * all the {@link AppliedPTransform AppliedPTransforms} that consume them at construction time.
 *
 * <p>Whenever a root {@link AppliedPTransform transform} produces elements, the
 * {@link InMemoryWatermarkManager} is provided with the produced elements and the output watermark
 * of the producing {@link AppliedPTransform transform}. The
 * {@link InMemoryWatermarkManager watermark manager} is responsible for computing the watermarks
 * of all {@link AppliedPTransform transforms} that consume one or more
 * {@link PCollection PCollections}.
 *
 * <p>Whenever a non-root {@link AppliedPTransform} finishes processing one or more in-flight
 * elements (referred to as the input {@link Bundle bundle}), the following occurs atomically:
 * <ul>
 *  <li>All of the in-flight elements are removed from the collection of pending elements for the
 *      {@link AppliedPTransform}.</li>
 *  <li>All of the elements produced by the {@link AppliedPTransform} are added to the collection
 *      of pending elements for each {@link AppliedPTransform} that consumes them.</li>
 *  <li>The input watermark for the {@link AppliedPTransform} becomes the maximum value of
 *    <ul>
 *      <li>the previous input watermark</li>
 *      <li>the minimum of
 *        <ul>
 *          <li>the timestamps of all currently pending elements</li>
 *          <li>all input {@link PCollection} watermarks</li>
 *        </ul>
 *      </li>
 *    </ul>
 *  </li>
 *  <li>The output watermark for the {@link AppliedPTransform} becomes the maximum of
 *    <ul>
 *      <li>the previous output watermark</li>
 *      <li>the minimum of
 *        <ul>
 *          <li>the current input watermark</li>
 *          <li>the current watermark holds</li>
 *        </ul>
 *      </li>
 *    </ul>
 *  </li>
 *  <li>The watermark of the output {@link PCollection} can be advanced to the output watermark of
 *      the {@link AppliedPTransform}</li>
 *  <li>The watermark of all downstream {@link AppliedPTransform AppliedPTransforms} can be
 *      advanced.</li>
 * </ul>
 *
 * <p>The watermark of a {@link PCollection} is equal to the output watermark of the
 * {@link AppliedPTransform} that produces it.
 *
 * <p>The watermarks for a {@link PTransform} are updated as follows when output is committed:<pre>
 * Watermark_In'  = MAX(Watermark_In, MIN(U(TS_Pending), U(Watermark_InputPCollection)))
 * Watermark_Out' = MAX(Watermark_Out, MIN(Watermark_In', U(StateHold)))
 * Watermark_PCollection = Watermark_Out_ProducingPTransform
 * </pre>
 */
public class InMemoryWatermarkManager {
  /**
   * The watermark of some {@link Pipeline} element, usually a {@link PTransform} or a
   * {@link PCollection}.
   *
   * <p>A watermark is a monotonically increasing value, which represents the point up to which the
   * system believes it has received all of the data. Data that arrives with a timestamp that is
   * before the watermark is considered late. {@link BoundedWindow#TIMESTAMP_MAX_VALUE} is a special
   * timestamp which indicates we have received all of the data and there will be no more on-time or
   * late data. This value is represented by {@link InMemoryWatermarkManager#THE_END_OF_TIME}.
   */
  private static interface Watermark {
    /**
     * Returns the current value of this watermark.
     */
    Instant get();

    /**
     * Refreshes the value of this watermark from its input watermarks and watermark holds.
     *
     * @return true if the value of the watermark has changed (and thus dependent watermark must
     *         also be updated
     */
    WatermarkUpdate refresh();

  }

  /**
   * The result of computing a {@link Watermark}.
   */
  private static enum WatermarkUpdate {
    /** The watermark is later than the value at the previous time it was computed. */
    ADVANCED(true),
    /** The watermark is equal to the value at the previous time it was computed. */
    NO_CHANGE(false);

    private final boolean advanced;

    private WatermarkUpdate(boolean advanced) {
      this.advanced = advanced;
    }

    public boolean isAdvanced() {
      return advanced;
    }

    /**
     * Returns the {@link WatermarkUpdate} based on the former and current
     * {@link Instant timestamps}.
     */
    public static WatermarkUpdate fromTimestamps(Instant oldTime, Instant currentTime) {
      if (currentTime.isAfter(oldTime)) {
        return ADVANCED;
      }
      return NO_CHANGE;
    }
  }

  /**
   * The input {@link Watermark} of an {@link AppliedPTransform}.
   *
   * <p>At any point, the value of an {@link AppliedPTransformInputWatermark} is equal to the
   * minimum watermark across all of its input {@link Watermark Watermarks}, and the minimum
   * timestamp of all of the pending elements, restricted to be monotonically increasing.
   *
   * <p>See {@link #refresh()} for more information.
   */
  private static class AppliedPTransformInputWatermark implements Watermark {
    private final Collection<? extends Watermark> inputWatermarks;
    private final SortedMultiset<WindowedValue<?>> pendingElements;
    private AtomicReference<Instant> currentWatermark;

    public AppliedPTransformInputWatermark(Collection<? extends Watermark> inputWatermarks) {
      this.inputWatermarks = inputWatermarks;
      this.pendingElements = TreeMultiset.create(PENDING_ELEMENT_COMPARATOR);
      currentWatermark = new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @Override
    public Instant get() {
      return currentWatermark.get();
    }

    /**
     * {@inheritDoc}.
     *
     * <p>When refresh is called, the value of the {@link AppliedPTransformInputWatermark} becomes
     * equal to the maximum value of
     * <ul>
     *   <li>the previous input watermark</li>
     *   <li>the minimum of
     *     <ul>
     *       <li>the timestamps of all currently pending elements</li>
     *       <li>all input {@link PCollection} watermarks</li>
     *     </ul>
     *   </li>
     * </ul>
     */
    @Override
    public synchronized WatermarkUpdate refresh() {
      Instant oldWatermark = currentWatermark.get();
      Instant minInputWatermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
      for (Watermark inputWatermark : inputWatermarks) {
        minInputWatermark = INSTANT_ORDERING.min(minInputWatermark, inputWatermark.get());
      }
      if (!pendingElements.isEmpty()) {
        minInputWatermark = INSTANT_ORDERING.min(
            minInputWatermark, pendingElements.firstEntry().getElement().getTimestamp());
      }
      Instant newWatermark = INSTANT_ORDERING.max(oldWatermark, minInputWatermark);
      currentWatermark.set(newWatermark);
      return WatermarkUpdate.fromTimestamps(oldWatermark, newWatermark);
    }

    public synchronized void addPending(Iterable<? extends WindowedValue<?>> newPending) {
      for (WindowedValue<?> pendingElement : newPending) {
        pendingElements.add(pendingElement);
      }
    }

    public synchronized void removePending(Iterable<? extends WindowedValue<?>> finishedElements) {
      for (WindowedValue<?> finishedElement : finishedElements) {
        pendingElements.remove(finishedElement);
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(AppliedPTransformInputWatermark.class)
          .add("pendingElements", pendingElements)
          .add("currentWatermark", currentWatermark)
          .toString();
    }
  }

  /**
   * The output {@link Watermark} of an {@link AppliedPTransform}.
   *
   * <p>The value of an {@link AppliedPTransformOutputWatermark} is equal to the minimum of the
   * current watermark hold and the {@link AppliedPTransformInputWatermark} for the same
   * {@link AppliedPTransform}, restricted to be monotonically increasing. See
   * {@link #refresh()} for more information.
   */
  private static class AppliedPTransformOutputWatermark implements Watermark {
    private final Watermark inputWatermark;
    private Instant currentHold;
    private AtomicReference<Instant> currentWatermark;

    public AppliedPTransformOutputWatermark(AppliedPTransformInputWatermark inputWatermark) {
      this.inputWatermark = inputWatermark;
      currentHold = BoundedWindow.TIMESTAMP_MAX_VALUE;
      currentWatermark = new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    public synchronized void setHold(Instant newHold) {
      currentHold = newHold;
    }

    @Override
    public Instant get() {
      return currentWatermark.get();
    }

    /**
     * {@inheritDoc}.
     *
     * <p>When refresh is called, the value of the {@link AppliedPTransformOutputWatermark} becomes
     * equal to the maximum value of:
     * <ul>
     *   <li>the previous output watermark</li>
     *   <li>the minimum of
     *     <ul>
     *       <li>the current input watermark</li>
     *       <li>the current watermark holds</li>
     *     </ul>
     *   </li>
     * </ul>
     */
    @Override
    public synchronized WatermarkUpdate refresh() {
      Instant oldWatermark = currentWatermark.get();
      Instant newWatermark;
      if (currentHold == null) {
        newWatermark = inputWatermark.get();
      } else {
        newWatermark = INSTANT_ORDERING.min(inputWatermark.get(), currentHold);
      }
      newWatermark = INSTANT_ORDERING.max(oldWatermark, newWatermark);
      currentWatermark.set(newWatermark);
      return WatermarkUpdate.fromTimestamps(oldWatermark, newWatermark);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(AppliedPTransformOutputWatermark.class)
          .add("currentHold", currentHold)
          .add("currentWatermark", currentWatermark)
          .toString();
    }
  }

  /**
   * A {@code Watermark} that is after the latest time it is possible to represent in the global
   * window. This is a distinguished value representing a complete {@link PTransform}.
   */
  private static final Watermark THE_END_OF_TIME = new Watermark() {
    @Override
    public WatermarkUpdate refresh() {
      // THE_END_OF_TIME is a distinguished value that cannot be advanced.
      return WatermarkUpdate.NO_CHANGE;
    }

    @Override
    public Instant get() {
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
  };

  private static final Ordering<Instant> INSTANT_ORDERING = Ordering.natural();

  /**
   * An ordering that compares windowed values by timestamp, then arbitrarily. This ensures that
   * {@link WindowedValue WindowedValues} will be sorted by timestamp, while two different
   * {@link WindowedValue WindowedValues} with the same timestamp are not considered equal.
   */
  private static final Ordering<WindowedValue<? extends Object>> PENDING_ELEMENT_COMPARATOR =
      (new WindowedValueByTimestampComparator()).compound(Ordering.arbitrary());

  /**
   * A map from each {@link PCollection} to all {@link AppliedPTransform PTransform applications}
   * that consume that {@link PCollection}.
   */
  private final Map<PCollection<?>, Collection<AppliedPTransform<?, ?, ?>>> consumers;

  /**
   * The input and output watermark of each {@link AppliedPTransform}.
   */
  private final Map<AppliedPTransform<?, ?, ?>, TransformWatermarks> transformToWatermarks;

  /**
   * Creates a new {@link InMemoryWatermarkManager}. All watermarks within the newly created
   * {@link InMemoryWatermarkManager} start at {@link BoundedWindow#TIMESTAMP_MIN_VALUE}, the
   * minimum watermark, with no watermark holds or pending elements.
   *
   * @param rootTransforms the root-level transforms of the {@link Pipeline}
   * @param consumers a mapping between each {@link PCollection} in the {@link Pipeline} to the
   *                  transforms that consume it as a part of their input
   */
  public static InMemoryWatermarkManager create(
      Collection<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<PCollection<?>, Collection<AppliedPTransform<?, ?, ?>>> consumers) {
    return new InMemoryWatermarkManager(rootTransforms, consumers);
  }

  private InMemoryWatermarkManager(
      Collection<AppliedPTransform<?, ?, ?>> rootTransforms,
      Map<PCollection<?>, Collection<AppliedPTransform<?, ?, ?>>> consumers) {
    this.consumers = consumers;

    transformToWatermarks = new HashMap<>();

    for (AppliedPTransform<?, ?, ?> rootTransform : rootTransforms) {
      getTransformWatermark(rootTransform);
    }
    for (Collection<AppliedPTransform<?, ?, ?>> intermediateTransforms : consumers.values()) {
      for (AppliedPTransform<?, ?, ?> transform : intermediateTransforms) {
        getTransformWatermark(transform);
      }
    }
  }

  private TransformWatermarks getTransformWatermark(AppliedPTransform<?, ?, ?> transform) {
    TransformWatermarks wms = transformToWatermarks.get(transform);
    if (wms == null) {
      List<Watermark> inputCollectionWatermarks = getInputWatermarks(transform);
      AppliedPTransformInputWatermark inputWatermark =
          new AppliedPTransformInputWatermark(inputCollectionWatermarks);
      AppliedPTransformOutputWatermark outputWatermark =
          new AppliedPTransformOutputWatermark(inputWatermark);
      wms = new TransformWatermarks(inputWatermark, outputWatermark);
      transformToWatermarks.put(transform, wms);
    }
    return wms;
  }

  private List<Watermark> getInputWatermarks(AppliedPTransform<?, ?, ?> transform) {
    ImmutableList.Builder<Watermark> inputWatermarksBuilder = ImmutableList.builder();
    Collection<? extends PValue> inputs = transform.getInput().expand();
    if (inputs.isEmpty()) {
      inputWatermarksBuilder.add(THE_END_OF_TIME);
    }
    for (PValue pvalue : inputs) {
      Watermark producerOutputWatermark =
          getTransformWatermark(pvalue.getProducingTransformInternal()).outputWatermark();
      inputWatermarksBuilder.add(producerOutputWatermark);
    }
    List<Watermark> inputCollectionWatermarks = inputWatermarksBuilder.build();
    return inputCollectionWatermarks;
  }

  /**
   * Gets the input and output watermarks for an {@link AppliedPTransform}. If the
   * {@link AppliedPTransform PTransform} has not processed any elements, return a watermark of
   * {@link BoundedWindow#TIMESTAMP_MIN_VALUE}.
   *
   * @return a snapshot of the input watermark and output watermark for the provided transform
   */
  public synchronized TransformWatermarks getWatermarks(AppliedPTransform<?, ?, ?> transform) {
    return transformToWatermarks.get(transform);
  }

  /**
   * Updates the output watermark of a transform that takes no input.
   *
   * <p>The output watermark of a transform that takes no input is determined by that transform, as
   * there are no input {@link PCollection PCollections}.
   *
   * @param watermark the output watermark of the transform. If the transform has buffered input
   *                  elements, the watermark should be the minimum of all buffered elements.
   * @return both watermarks of the source transform
   */
  public TransformWatermarks updateOutputWatermark(
      AppliedPTransform<?, ?, ?> transform, Iterable<Bundle<?>> outputs, Instant watermark) {
    TransformWatermarks watermarks = getWatermarks(transform);
    watermarks.outputWatermark().setHold(watermark);

    for (Bundle<?> output : outputs) {
      PCollection<?> pCollection = output.getPCollection();
      for (AppliedPTransform<?, ?, ?> consumer : consumers.get(pCollection)) {
        addPending(consumer, output.getElements());
      }
    }
    refreshWatermarks(transform);
    return watermarks;
  }

  /**
   * Updates the watermarks of a transform with one or more inputs.
   *
   * <p>Each transform has two monotonically increasing watermarks: the input watermark, which can,
   * at any time, be updated to equal:
   * <pre>
   * MAX(CurrentInputWatermark, MIN(PendingElements, InputPCollectionWatermarks))
   * </pre>
   * and the output watermark, which can, at any time, be updated to equal:
   * <pre>
   * MAX(CurrentOutputWatermark, MIN(InputWatermark, WatermarkHolds))
   * </pre>.
   *
   * @param completed the input that has completed
   * @param transform the transform that has completed processing the input
   * @param outputs the bundles the transform has output
   * @param earliestHold the earliest watermark hold in the transform's state. {@code null} if there
   *                     is no hold
   * @return the updated watermark of the transform
   */
  public TransformWatermarks updateWatermarks(Bundle<?> completed,
      AppliedPTransform<?, ?, ?> transform, Collection<Bundle<?>> outputs,
      @Nullable Instant earliestHold) {
    updatePending(completed, transform, outputs);
    TransformWatermarks transformWms = transformToWatermarks.get(transform);
    transformWms.outputWatermark().setHold(earliestHold);
    refreshWatermarks(transform);
    return transformWms;
  }

  private void refreshWatermarks(AppliedPTransform<?, ?, ?> transform) {
    TransformWatermarks myWatermarks = transformToWatermarks.get(transform);
    WatermarkUpdate updateResult = myWatermarks.refresh();
    if (updateResult.isAdvanced()) {
      for (PValue outputPValue : transform.getOutput().expand()) {
        Collection<AppliedPTransform<?, ?, ?>> downstreamTransforms = consumers.get(outputPValue);
        if (downstreamTransforms != null) {
          for (AppliedPTransform<?, ?, ?> downstreamTransform : downstreamTransforms) {
            refreshWatermarks(downstreamTransform);
          }
        }
      }
    }
  }

  /**
   * Removes all elements consumed by the input bundle from the {@link PTransform PTransforms}
   * collection of pending elements, and adds all elements produced by the {@link PTransform} to the
   * pending queue of each consumer.
   */
  private void updatePending(
      Bundle<?> input, AppliedPTransform<?, ?, ?> transform, Collection<Bundle<?>> outputs) {
    AppliedPTransformInputWatermark inputWatermark =
        transformToWatermarks.get(transform).inputWatermark();
    inputWatermark.removePending(input.getElements());

    for (Bundle<?> bundle : outputs) {
      for (AppliedPTransform<?, ?, ?> consumer : consumers.get(bundle.getPCollection())) {
        addPending(consumer, bundle.getElements());
      }
    }
  }

  /**
   * Adds all of the provided {@link WindowedValue WindowedValues} to the collection of pending
   * elements for the provided {@link AppliedPTransform}.
   */
  private void addPending(
      AppliedPTransform<?, ?, ?> transform, Iterable<? extends WindowedValue<?>> pending) {
    TransformWatermarks watermarks = transformToWatermarks.get(transform);
    watermarks.inputWatermark().addPending(pending);
  }

  /**
   * A reference to the input and output watermarks of an {@link AppliedPTransform}.
   */
  public class TransformWatermarks {
    private final AppliedPTransformInputWatermark inputWatermark;
    private final AppliedPTransformOutputWatermark outputWatermark;

    private TransformWatermarks(
        AppliedPTransformInputWatermark inputWatermark,
        AppliedPTransformOutputWatermark outputWatermark) {
      this.inputWatermark = inputWatermark;
      this.outputWatermark = outputWatermark;
    }

    /**
     * Returns the input watermark of the {@link AppliedPTransform}.
     */
    public Instant getInputWatermark() {
      return inputWatermark.get();
    }

    /**
     * Returns the output watermark of the {@link AppliedPTransform}.
     */
    public Instant getOutputWatermark() {
      return outputWatermark.get();
    }

    AppliedPTransformInputWatermark inputWatermark() {
      return inputWatermark;
    }

    AppliedPTransformOutputWatermark outputWatermark() {
      return outputWatermark;
    }

    private WatermarkUpdate refresh() {
      inputWatermark.refresh();
      return outputWatermark.refresh();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(TransformWatermarks.class)
          .add("inputWatermark", inputWatermark)
          .add("outputWatermark", outputWatermark)
          .toString();
    }
  }

  private static class WindowedValueByTimestampComparator extends Ordering<WindowedValue<?>> {
    @Override
    public int compare(WindowedValue<?> o1, WindowedValue<?> o2) {
      return o1.getTimestamp().compareTo(o2.getTimestamp());
    }
  }
}
