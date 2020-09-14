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
package org.apache.beam.runners.fnexecution.translation;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.fnexecution.control.TimerReceiverFactory;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;

/** Utilities for pipeline translation. */
public final class PipelineTranslatorUtils {
  private PipelineTranslatorUtils() {}

  /** Creates a mapping from PCollection id to output tag integer. */
  public static BiMap<String, Integer> createOutputMap(Iterable<String> localOutputs) {
    ImmutableBiMap.Builder<String, Integer> builder = ImmutableBiMap.builder();
    int outputIndex = 0;
    // sort localOutputs for stable indexing
    for (String tag : Sets.newTreeSet(localOutputs)) {
      builder.put(tag, outputIndex);
      outputIndex++;
    }
    return builder.build();
  }

  /** Creates a coder for a given PCollection id from the Proto definition. */
  public static <T> Coder<WindowedValue<T>> instantiateCoder(
      String collectionId, RunnerApi.Components components) {
    PipelineNode.PCollectionNode collectionNode =
        PipelineNode.pCollection(collectionId, components.getPcollectionsOrThrow(collectionId));
    try {
      return WireCoders.instantiateRunnerWireCoder(collectionNode, components);
    } catch (IOException e) {
      throw new RuntimeException("Could not instantiate Coder", e);
    }
  }

  public static WindowingStrategy getWindowingStrategy(
      String pCollectionId, RunnerApi.Components components) {
    RunnerApi.WindowingStrategy windowingStrategyProto =
        components.getWindowingStrategiesOrThrow(
            components.getPcollectionsOrThrow(pCollectionId).getWindowingStrategyId());

    try {
      return WindowingStrategyTranslation.fromProto(
          windowingStrategyProto, RehydratedComponents.forComponents(components));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          String.format(
              "Unable to hydrate windowing strategy %s for %s.",
              windowingStrategyProto, pCollectionId),
          e);
    }
  }

  /** Indicates whether the given pipeline has any unbounded PCollections. */
  public static boolean hasUnboundedPCollections(RunnerApi.Pipeline pipeline) {
    checkNotNull(pipeline);
    Collection<PCollection> pCollecctions = pipeline.getComponents().getPcollectionsMap().values();
    // Assume that all PCollections are consumed at some point in the pipeline.
    return pCollecctions.stream()
        .anyMatch(pc -> pc.getIsBounded() == RunnerApi.IsBounded.Enum.UNBOUNDED);
  }

  /**
   * Fires all timers which are ready to be fired. This is done in a loop because timers may itself
   * schedule timers.
   */
  public static void fireEligibleTimers(
      InMemoryTimerInternals timerInternals,
      Map<KV<String, String>, FnDataReceiver<Timer>> timerReceivers,
      Object currentTimerKey) {

    boolean hasFired;
    do {
      hasFired = false;
      TimerInternals.TimerData timer;

      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        hasFired = true;
        fireTimer(timer, timerReceivers, currentTimerKey);
      }
      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        hasFired = true;
        fireTimer(timer, timerReceivers, currentTimerKey);
      }
      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
        hasFired = true;
        fireTimer(timer, timerReceivers, currentTimerKey);
      }
    } while (hasFired);
  }

  private static void fireTimer(
      TimerInternals.TimerData timer,
      Map<KV<String, String>, FnDataReceiver<Timer>> timerReceivers,
      Object currentTimerKey) {
    StateNamespace namespace = timer.getNamespace();
    Preconditions.checkArgument(namespace instanceof StateNamespaces.WindowNamespace);
    BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
    Instant timestamp = timer.getTimestamp();
    Instant outputTimestamp = timer.getOutputTimestamp();
    Timer<?> timerValue =
        Timer.of(
            currentTimerKey,
            "",
            Collections.singletonList(window),
            timestamp,
            outputTimestamp,
            PaneInfo.NO_FIRING);
    KV<String, String> transformAndTimerId =
        TimerReceiverFactory.decodeTimerDataTimerId(timer.getTimerId());
    FnDataReceiver<Timer> fnTimerReceiver = timerReceivers.get(transformAndTimerId);
    Preconditions.checkNotNull(
        fnTimerReceiver, "No FnDataReceiver found for %s", transformAndTimerId);
    try {
      fnTimerReceiver.accept(timerValue);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(Locale.ENGLISH, "Failed to process timer: %s", timerValue));
    }
  }

  public static <T> WindowedValue.WindowedValueCoder<T> getWindowedValueCoder(
      String pCollectionId, RunnerApi.Components components) {
    RunnerApi.PCollection pCollection = components.getPcollectionsOrThrow(pCollectionId);
    PipelineNode.PCollectionNode pCollectionNode =
        PipelineNode.pCollection(pCollectionId, pCollection);
    WindowedValue.WindowedValueCoder<T> coder;
    try {
      coder =
          (WindowedValue.WindowedValueCoder)
              WireCoders.instantiateRunnerWireCoder(pCollectionNode, components);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return coder;
  }

  public static String getInputId(PipelineNode.PTransformNode transformNode) {
    return Iterables.getOnlyElement(transformNode.getTransform().getInputsMap().values());
  }

  public static String getOutputId(PipelineNode.PTransformNode transformNode) {
    return Iterables.getOnlyElement(transformNode.getTransform().getOutputsMap().values());
  }

  public static String getExecutableStageIntermediateId(PipelineNode.PTransformNode transformNode) {
    return transformNode.getId();
  }
}
