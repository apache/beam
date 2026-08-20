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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import java.util.function.Supplier;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state.BeamStatefulProcessorConfig;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Shared plumbing of the three streaming translators: the guards that reject Beam features this POC
 * deliberately does not implement, and the small serializable functions that convert between the
 * runner's {@code Dataset<WindowedValue<T>>} representation and the raw {@code byte[]} row layouts
 * of {@link TwsTransformFactory}.
 *
 * <p>Guards throw {@link UnsupportedOperationException} naming the offending feature. They run at
 * translation time, before any Spark query is started, so an unsupported pipeline fails immediately
 * and loudly rather than producing quietly wrong results.
 */
final class StreamingTranslationHelpers {

  private StreamingTranslationHelpers() {}

  // -----------------------------------------------------------------------------------------
  //  Guards
  // -----------------------------------------------------------------------------------------

  /**
   * Rejects windowing strategies whose semantics the {@code transformWithState} bridge cannot
   * reproduce: merging (session) windows, custom triggers and accumulating panes.
   */
  static void checkSupportedWindowing(WindowingStrategy<?, ?> strategy, String stepName) {
    WindowFn<?, ?> windowFn = strategy.getWindowFn();
    if (!windowFn.isNonMerging()) {
      throw unsupported(
          stepName,
          "merging windows ("
              + windowFn.getClass().getSimpleName()
              + "). Session windows and any other merging WindowFn are out of scope for the "
              + "Spark 4 streaming runner");
    }
    Trigger trigger = strategy.getTrigger();
    if (!isDefaultTrigger(trigger)) {
      throw unsupported(
          stepName,
          "the custom trigger "
              + trigger
              + ". Only the default trigger (one on-time pane per window when the watermark passes "
              + "its end) is implemented");
    }
    if (strategy.getMode() != AccumulationMode.DISCARDING_FIRED_PANES) {
      throw unsupported(
          stepName,
          "accumulating panes (" + strategy.getMode() + "). Only discarding panes are implemented");
    }
  }

  /**
   * The default trigger, as Beam models it, is either {@link DefaultTrigger} itself, the equivalent
   * {@code AfterWatermark.pastEndOfWindow()} without early or late firings, or {@link
   * Never.NeverTrigger} used by {@code PAssert} on unbounded streams to gather all window contents
   * before the end-of-stream watermark triggers final pane evaluation.
   */
  private static boolean isDefaultTrigger(Trigger trigger) {
    if (trigger instanceof DefaultTrigger) {
      return true;
    }
    if (trigger instanceof AfterWatermark.FromEndOfWindow) {
      // FromEndOfWindow without early/late firings is exactly the default trigger; with firings
      // configured Beam represents it as one of the AfterWatermarkEarlyAndLate subclasses instead.
      return true;
    }
    if (trigger instanceof Never.NeverTrigger) {
      // NeverTrigger is used by PAssert on unbounded PCollections to prevent intermediate firings.
      // In this runner, end-of-stream watermark advancement fires the final pane.
      return true;
    }
    return false;
  }

  /**
   * The Beam key is the Spark grouping key and is compared as raw bytes, so two equal keys must
   * always encode identically.
   */
  static void checkDeterministicKeyCoder(Coder<?> keyCoder, String stepName) {
    try {
      keyCoder.verifyDeterministic();
    } catch (Coder.NonDeterministicException e) {
      throw new UnsupportedOperationException(
          "Cannot translate "
              + stepName
              + " for streaming: the key coder "
              + keyCoder
              + " is not deterministic. Keys are grouped by their encoded bytes, so a "
              + "non-deterministic key coder would silently split a single Beam key across "
              + "several Spark state entries.",
          e);
    }
  }

  /**
   * Only event time timers reach the {@code transformWithState} operator, which runs in {@code
   * TimeMode.EventTime()}; a processing time timer would never fire.
   */
  static void checkNoProcessingTimeTimers(
      DoFn<?, ?> doFn, DoFnSignature signature, String stepName) {
    for (DoFnSignature.TimerDeclaration timer : signature.timerDeclarations().values()) {
      TimerSpec spec = DoFnSignatures.getTimerSpecOrThrow(timer, doFn);
      if (spec.getTimeDomain() != TimeDomain.EVENT_TIME) {
        throw unsupported(
            stepName,
            "the "
                + spec.getTimeDomain()
                + " timer @TimerId(\""
                + timer.id()
                + "\"). Only event time timers are implemented");
      }
    }
    for (DoFnSignature.TimerFamilyDeclaration family :
        signature.timerFamilyDeclarations().values()) {
      TimerSpec spec = DoFnSignatures.getTimerFamilySpecOrThrow(family, doFn);
      if (spec.getTimeDomain() != TimeDomain.EVENT_TIME) {
        throw unsupported(
            stepName,
            "the "
                + spec.getTimeDomain()
                + " timer family @TimerFamily(\""
                + family.id()
                + "\"). Only event time timers are implemented");
      }
    }
  }

  static UnsupportedOperationException unsupported(String stepName, String feature) {
    return new UnsupportedOperationException(
        "Cannot translate " + stepName + " for streaming, it uses " + feature + ".");
  }

  // -----------------------------------------------------------------------------------------
  //  Row conversions
  // -----------------------------------------------------------------------------------------

  /** Adapts the translation context's options supplier to the shape the operator config wants. */
  static BeamStatefulProcessorConfig.OptionsSupplier optionsSupplier(
      Supplier<PipelineOptions> supplier) {
    return new DelegatingOptionsSupplier(supplier);
  }

  private static final class DelegatingOptionsSupplier
      implements BeamStatefulProcessorConfig.OptionsSupplier {
    private final Supplier<PipelineOptions> delegate;

    DelegatingOptionsSupplier(Supplier<PipelineOptions> delegate) {
      this.delegate = delegate;
    }

    @Override
    public PipelineOptions get() {
      return delegate.get();
    }
  }

  /**
   * Turns {@code WindowedValue<KV<K, V>>} into a {@link TwsTransformFactory} input row: the encoded
   * key followed by the {@code WindowedValue} of the value side only.
   */
  static final class EncodeKeyedRow<K, V> implements MapFunction<WindowedValue<KV<K, V>>, byte[]> {
    private final Coder<K> keyCoder;
    private final Coder<WindowedValue<V>> payloadCoder;

    EncodeKeyedRow(Coder<K> keyCoder, Coder<WindowedValue<V>> payloadCoder) {
      this.keyCoder = keyCoder;
      this.payloadCoder = payloadCoder;
    }

    @Override
    public byte[] call(WindowedValue<KV<K, V>> element) {
      KV<K, V> kv = element.getValue();
      return TwsTransformFactory.encodeInputRow(
          CoderHelpers.toByteArray(kv.getKey(), keyCoder),
          CoderHelpers.toByteArray(element.withValue(kv.getValue()), payloadCoder));
    }
  }

  /** Decodes the {@code WindowedValue} payload of a {@link TwsTransformFactory} output row. */
  static final class DecodeTaggedOutput<T> implements MapFunction<byte[], WindowedValue<T>> {
    private final Coder<WindowedValue<T>> coder;

    DecodeTaggedOutput(Coder<WindowedValue<T>> coder) {
      this.coder = coder;
    }

    @Override
    public WindowedValue<T> call(byte[] row) {
      return CoderHelpers.fromByteArray(TwsTransformFactory.outputPayload(row), coder);
    }
  }

  /** Keeps only the {@link TwsTransformFactory} output rows carrying one specific tag index. */
  static final class TagIndexFilter implements FilterFunction<byte[]> {
    private final int tagIndex;

    TagIndexFilter(int tagIndex) {
      this.tagIndex = tagIndex;
    }

    @Override
    public boolean call(byte[] row) {
      return TwsTransformFactory.outputTagIndex(row) == tagIndex;
    }
  }
}
