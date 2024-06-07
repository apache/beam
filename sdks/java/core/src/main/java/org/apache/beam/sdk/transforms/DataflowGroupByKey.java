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
package org.apache.beam.sdk.transforms;

import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark.AfterWatermarkEarlyAndLate;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark.FromEndOfWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Never.NeverTrigger;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Specialized implementation of {@code GroupByKey} for translating Redistribute transform to
 * Dataflow pipelines.
 */

public class DataflowGroupByKey<K, V>
    extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

  private final boolean fewKeys;
  // Plumbed from Redistribute transform.
  private final boolean allowDuplicates;

  private DataflowGroupByKey(boolean fewKeys, boolean allowDuplicates) {
    this.fewKeys = fewKeys;
    this.allowDuplicates = allowDuplicates;
  }

  /**
   * Returns a {@code DataflowGroupByKey<K, V>} {@code PTransform}.
   *
   * @param <K> the type of the keys of the input and output {@code PCollection}s
   * @param <V> the type of the values of the input {@code PCollection} and the elements of the
   *     {@code Iterable}s in the output {@code PCollection}
   */
  public static <K, V> DataflowGroupByKey<K, V> create() {
    return new DataflowGroupByKey<>(false, false);
  }

  /**
   * Returns a {@code DataflowGroupByKey<K, V>} {@code PTransform} that assumes it will be grouping
   * a small number of keys.
   *
   * @param <K> the type of the keys of the input and output {@code PCollection}s
   * @param <V> the type of the values of the input {@code PCollection} and the elements of the
   *     {@code Iterable}s in the output {@code PCollection}
   */
  static <K, V> DataflowGroupByKey<K, V> createWithFewKeys() {
    return new DataflowGroupByKey<>(true, false);
  }

  /**
   * Returns a {@code DataflowGroupByKey<K, V>} {@code PTransform} that its output can have
   * duplicated elements.
   *
   * @param <K> the type of the keys of the input and output {@code PCollection}s
   * @param <V> the type of the values of the input {@code PCollection} and the elements of the
   *     {@code Iterable}s in the output {@code PCollection}
   */
  public static <K, V> DataflowGroupByKey<K, V> createWithAllowDuplicates() {
    return new DataflowGroupByKey<>(false, true);
  }

  /** Returns whether it groups just few keys. */
  public boolean fewKeys() {
    return fewKeys;
  }

  /** Returns whether it allows duplicated elements in the output. */
  public boolean allowDuplicates() {
    return allowDuplicates;
  }

  /////////////////////////////////////////////////////////////////////////////

  public static void applicableTo(PCollection<?> input) {
    WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
    // Verify that the input PCollection is bounded, or that there is windowing/triggering being
    // used. Without this, the watermark (at end of global window) will never be reached.
    if (windowingStrategy.getWindowFn() instanceof GlobalWindows
        && windowingStrategy.getTrigger() instanceof DefaultTrigger
        && input.isBounded() != IsBounded.BOUNDED) {
      throw new IllegalStateException(
          "DataflowGroupByKey cannot be applied to non-bounded PCollection in the GlobalWindow"
              + " without a trigger. Use a Window.into or Window.triggering transform prior to"
              + " DataflowGroupByKey.");
    }

    // Validate that the trigger does not finish before garbage collection time
    if (!triggerIsSafe(windowingStrategy)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsafe trigger '%s' may lose data, did you mean to wrap it in"
                  + "`Repeatedly.forever(...)`?%nSee "
                  + "https://s.apache.org/finishing-triggers-drop-data "
                  + "for details.",
              windowingStrategy.getTrigger()));
    }
  }

  @Override
  public void validate(
      @Nullable PipelineOptions options,
      Map<TupleTag<?>, PCollection<?>> inputs,
      Map<TupleTag<?>, PCollection<?>> outputs) {
    PCollection<?> input = Iterables.getOnlyElement(inputs.values());
    KvCoder<K, V> inputCoder = getInputKvCoder(input.getCoder());

    // Ensure that the output coder key and value types aren't different.
    Coder<?> outputCoder = Iterables.getOnlyElement(outputs.values()).getCoder();
    KvCoder<?, ?> expectedOutputCoder = getOutputKvCoder(inputCoder);
    if (!expectedOutputCoder.equals(outputCoder)) {
      throw new IllegalStateException(
          String.format(
              "the DataflowGroupByKey requires its output coder to be %s but found %s.",
              expectedOutputCoder, outputCoder));
    }
  }

  // Note that Never trigger finishes *at* GC time so it is OK, and
  // AfterWatermark.fromEndOfWindow() finishes at end-of-window time so it is
  // OK if there is no allowed lateness.
  private static boolean triggerIsSafe(WindowingStrategy<?, ?> windowingStrategy) {
    if (!windowingStrategy.getTrigger().mayFinish()) {
      return true;
    }

    if (windowingStrategy.getTrigger() instanceof NeverTrigger) {
      return true;
    }

    if (windowingStrategy.getTrigger() instanceof FromEndOfWindow
        && windowingStrategy.getAllowedLateness().getMillis() == 0) {
      return true;
    }

    if (windowingStrategy.getTrigger() instanceof AfterWatermarkEarlyAndLate
        && windowingStrategy.getAllowedLateness().getMillis() == 0) {
      return true;
    }

    if (windowingStrategy.getTrigger() instanceof AfterWatermarkEarlyAndLate
        && ((AfterWatermarkEarlyAndLate) windowingStrategy.getTrigger()).getLateTrigger() != null) {
      return true;
    }

    return false;
  }

  public WindowingStrategy<?, ?> updateWindowingStrategy(WindowingStrategy<?, ?> inputStrategy) {
    // If the WindowFn was merging, set the bit to indicate it is already merged.
    // Switch to the continuation trigger associated with the current trigger.
    return inputStrategy
        .withAlreadyMerged(!inputStrategy.getWindowFn().isNonMerging())
        .withTrigger(inputStrategy.getTrigger().getContinuationTrigger());
  }

  @Override
  public PCollection<KV<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
    applicableTo(input);

    // Verify that the input Coder<KV<K, V>> is a KvCoder<K, V>, and that
    // the key coder is deterministic.
    Coder<K> keyCoder = getKeyCoder(input.getCoder());
    try {
      keyCoder.verifyDeterministic();
    } catch (NonDeterministicException e) {
      throw new IllegalStateException(
          "the keyCoder of a DataflowGroupByKey must be deterministic", e);
    }

    // This primitive operation groups by the combination of key and window,
    // merging windows as needed, using the windows assigned to the
    // key/value input elements and the window merge operation of the
    // window function associated with the input PCollection.
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(),
        updateWindowingStrategy(input.getWindowingStrategy()),
        input.isBounded(),
        getOutputKvCoder(input.getCoder()));
  }

  /**
   * Returns the {@code Coder} of the input to this transform, which should be a {@code KvCoder}.
   */
  @SuppressWarnings("unchecked")
  static <K, V> KvCoder<K, V> getInputKvCoder(Coder<?> inputCoder) {
    if (!(inputCoder instanceof KvCoder)) {
      throw new IllegalStateException("DataflowGroupByKey requires its input to use KvCoder");
    }
    return (KvCoder<K, V>) inputCoder;
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the {@code Coder} of the keys of the input to this transform, which is also used as the
   * {@code Coder} of the keys of the output of this transform.
   */
  public static <K, V> Coder<K> getKeyCoder(Coder<KV<K, V>> inputCoder) {
    return DataflowGroupByKey.<K, V>getInputKvCoder(inputCoder).getKeyCoder();
  }

  /** Returns the {@code Coder} of the values of the input to this transform. */
  public static <K, V> Coder<V> getInputValueCoder(Coder<KV<K, V>> inputCoder) {
    return DataflowGroupByKey.<K, V>getInputKvCoder(inputCoder).getValueCoder();
  }

  /** Returns the {@code Coder} of the {@code Iterable} values of the output of this transform. */
  static <K, V> Coder<Iterable<V>> getOutputValueCoder(Coder<KV<K, V>> inputCoder) {
    return IterableCoder.of(getInputValueCoder(inputCoder));
  }

  /** Returns the {@code Coder} of the output of this transform. */
  public static <K, V> KvCoder<K, Iterable<V>> getOutputKvCoder(Coder<KV<K, V>> inputCoder) {
    return KvCoder.of(getKeyCoder(inputCoder), getOutputValueCoder(inputCoder));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    if (fewKeys) {
      builder.add(DisplayData.item("fewKeys", true).withLabel("Has Few Keys"));
    }
  }
}
