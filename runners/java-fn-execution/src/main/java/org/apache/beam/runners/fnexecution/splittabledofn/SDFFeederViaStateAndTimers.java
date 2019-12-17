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
package org.apache.beam.runners.fnexecution.splittabledofn;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.List;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.util.Durations;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;

/**
 * Helper class for feeding element/restricton pairs into a {@link
 * PTransformTranslation#SPLITTABLE_PROCESS_ELEMENTS_URN} transform, implementing checkpointing
 * only, by using state and timers for storing the last element/restriction pair, similarly to
 * {@link org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn} but in a portable
 * fashion.
 */
public class SDFFeederViaStateAndTimers<InputT, RestrictionT> {
  private final Coder<BoundedWindow> windowCoder;
  private final Coder<WindowedValue<KV<InputT, RestrictionT>>> elementRestrictionWireCoder;

  private final StateInternals stateInternals;
  private final TimerInternals timerInternals;

  private StateNamespace stateNamespace;

  private final StateTag<ValueState<WindowedValue<KV<InputT, RestrictionT>>>> seedTag;
  private ValueState<WindowedValue<KV<InputT, RestrictionT>>> seedState;

  private final StateTag<ValueState<RestrictionT>> restrictionTag;
  private ValueState<RestrictionT> restrictionState;

  private StateTag<WatermarkHoldState> watermarkHoldTag =
      StateTags.makeSystemTagInternal(
          StateTags.<GlobalWindow>watermarkStateInternal("hold", TimestampCombiner.LATEST));
  private WatermarkHoldState holdState;

  private Instant inputTimestamp;
  private List<BundleApplication> primaryRoots;
  private List<DelayedBundleApplication> residualRoots;

  /** Initializes the feeder. */
  public SDFFeederViaStateAndTimers(
      StateInternals stateInternals,
      TimerInternals timerInternals,
      Coder<InputT> elementWireCoder,
      Coder<RestrictionT> restrictionWireCoder,
      Coder<BoundedWindow> windowCoder) {
    this.stateInternals = stateInternals;
    this.timerInternals = timerInternals;
    this.windowCoder = windowCoder;
    this.elementRestrictionWireCoder =
        FullWindowedValueCoder.of(KvCoder.of(elementWireCoder, restrictionWireCoder), windowCoder);
    this.seedTag = StateTags.value("seed", elementRestrictionWireCoder);
    this.restrictionTag = StateTags.value("restriction", restrictionWireCoder);
  }

  /** Passes the initial element/restriction pair. */
  public void seed(WindowedValue<KV<InputT, RestrictionT>> elementRestriction) {
    initState(
        StateNamespaces.window(
            windowCoder, Iterables.getOnlyElement(elementRestriction.getWindows())));
    seedState.write(elementRestriction);
    inputTimestamp = elementRestriction.getTimestamp();
  }

  /**
   * Resumes from a timer and returns the current element/restriction pair (with an up-to-date value
   * of the restriction).
   */
  public WindowedValue<KV<InputT, RestrictionT>> resume(TimerData timer) {
    initState(timer.getNamespace());
    WindowedValue<KV<InputT, RestrictionT>> seed = seedState.read();
    inputTimestamp = seed.getTimestamp();
    return seed.withValue(KV.of(seed.getValue().getKey(), restrictionState.read()));
  }

  /**
   * Commits the state and timers: clears both if no checkpoint happened, or adjusts the restriction
   * and sets a wake-up timer if a checkpoint happened.
   */
  public void commit() throws IOException {
    if (primaryRoots == null) {
      // No split - the call terminated.
      seedState.clear();
      restrictionState.clear();
      holdState.clear();
      return;
    }

    // For now can only happen on the first instruction which is SPLITTABLE_PROCESS_ELEMENTS.
    checkArgument(residualRoots.size() == 1, "More than 1 residual is unsupported for now");
    DelayedBundleApplication residual = residualRoots.get(0);

    ByteString encodedResidual = residual.getApplication().getElement();
    WindowedValue<KV<InputT, RestrictionT>> decodedResidual =
        elementRestrictionWireCoder.decode(encodedResidual.newInput());

    restrictionState.write(decodedResidual.getValue().getValue());

    Instant watermarkHold =
        residual.getApplication().getOutputWatermarksMap().isEmpty()
            ? inputTimestamp
            : new Instant(
                Iterables.getOnlyElement(
                    residual.getApplication().getOutputWatermarksMap().values()));
    checkArgument(
        !watermarkHold.isBefore(inputTimestamp),
        "Watermark hold %s can not be before input timestamp %s",
        watermarkHold,
        inputTimestamp);
    holdState.add(watermarkHold);

    Instant requestedWakeupTime =
        new Instant(
            System.currentTimeMillis() + Durations.toMillis(residual.getRequestedTimeDelay()));
    Instant wakeupTime =
        timerInternals.currentProcessingTime().isBefore(requestedWakeupTime)
            ? requestedWakeupTime
            : timerInternals.currentProcessingTime();

    // Set a timer to continue processing this element.
    timerInternals.setTimer(
        stateNamespace, "sdfContinuation", wakeupTime, TimeDomain.PROCESSING_TIME);
  }

  /** Signals that a split happened. */
  public void split(
      List<BundleApplication> primaryRoots, List<DelayedBundleApplication> residualRoots) {
    checkState(
        this.primaryRoots == null,
        "At most 1 split supported, however got new split (%s, %s) "
            + "in addition to existing (%s, %s)",
        primaryRoots,
        residualRoots,
        this.primaryRoots,
        this.residualRoots);
    this.primaryRoots = primaryRoots;
    this.residualRoots = residualRoots;
  }

  private void initState(StateNamespace ns) {
    stateNamespace = ns;
    seedState = stateInternals.state(ns, seedTag);
    restrictionState = stateInternals.state(ns, restrictionTag);
    holdState = stateInternals.state(ns, watermarkHoldTag);
  }
}
