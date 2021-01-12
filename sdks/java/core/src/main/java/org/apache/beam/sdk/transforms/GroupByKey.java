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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark.AfterWatermarkEarlyAndLate;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark.FromEndOfWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.InvalidWindows;
import org.apache.beam.sdk.transforms.windowing.Never.NeverTrigger;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * {@code GroupByKey<K, V>} takes a {@code PCollection<KV<K, V>>}, groups the values by key and
 * windows, and returns a {@code PCollection<KV<K, Iterable<V>>>} representing a map from each
 * distinct key and window of the input {@code PCollection} to an {@code Iterable} over all the
 * values associated with that key in the input per window. Absent repeatedly-firing {@link
 * Window#triggering triggering}, each key in the output {@code PCollection} is unique within each
 * window.
 *
 * <p>{@code GroupByKey} is analogous to converting a multi-map into a uni-map, and related to
 * {@code GROUP BY} in SQL. It corresponds to the "shuffle" step between the Mapper and the Reducer
 * in the MapReduce framework.
 *
 * <p>Two keys of type {@code K} are compared for equality <b>not</b> by regular Java {@link
 * Object#equals}, but instead by first encoding each of the keys using the {@code Coder} of the
 * keys of the input {@code PCollection}, and then comparing the encoded bytes. This admits
 * efficient parallel evaluation. Note that this requires that the {@code Coder} of the keys be
 * deterministic (see {@link Coder#verifyDeterministic()}). If the key {@code Coder} is not
 * deterministic, an exception is thrown at pipeline construction time.
 *
 * <p>By default, the {@code Coder} of the keys of the output {@code PCollection} is the same as
 * that of the keys of the input, and the {@code Coder} of the elements of the {@code Iterable}
 * values of the output {@code PCollection} is the same as the {@code Coder} of the values of the
 * input.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<KV<String, Doc>> urlDocPairs = ...;
 * PCollection<KV<String, Iterable<Doc>>> urlToDocs =
 *     urlDocPairs.apply(GroupByKey.<String, Doc>create());
 * PCollection<R> results =
 *     urlToDocs.apply(ParDo.of(new DoFn<KV<String, Iterable<Doc>>, R>() }{
 *      {@code @ProcessElement
 *       public void processElement(ProcessContext c) {
 *         String url = c.element().getKey();
 *         Iterable<Doc> docsWithThatUrl = c.element().getValue();
 *         ... process all docs having that url ...
 *       }}}));
 * </pre>
 *
 * <p>{@code GroupByKey} is a key primitive in data-parallel processing, since it is the main way to
 * efficiently bring associated data together into one location. It is also a key determiner of the
 * performance of a data-parallel pipeline.
 *
 * <p>See {@link org.apache.beam.sdk.transforms.join.CoGroupByKey} for a way to group multiple input
 * PCollections by a common key at once.
 *
 * <p>See {@link Combine.PerKey} for a common pattern of {@code GroupByKey} followed by {@link
 * Combine.GroupedValues}.
 *
 * <p>When grouping, windows that can be merged according to the {@link WindowFn} of the input
 * {@code PCollection} will be merged together, and a window pane corresponding to the new, merged
 * window will be created. The items in this pane will be emitted when a trigger fires. By default
 * this will be when the input sources estimate there will be no more data for the window. See
 * {@link org.apache.beam.sdk.transforms.windowing.AfterWatermark} for details on the estimation.
 *
 * <p>The timestamp for each emitted pane is determined by the {@link
 * Window#withTimestampCombiner(TimestampCombiner)} windowing operation}. The output {@code
 * PCollection} will have the same {@link WindowFn} as the input.
 *
 * <p>If the input {@code PCollection} contains late data or the {@link Window#triggering requested
 * TriggerFn} can fire before the watermark, then there may be multiple elements output by a {@code
 * GroupByKey} that correspond to the same key and window.
 *
 * <p>If the {@link WindowFn} of the input requires merging, it is not valid to apply another {@code
 * GroupByKey} without first applying a new {@link WindowFn} or applying {@link Window#remerge()}.
 *
 * @param <K> the type of the keys of the input and output {@code PCollection}s
 * @param <V> the type of the values of the input {@code PCollection} and the elements of the {@code
 *     Iterable}s in the output {@code PCollection}
 */
public class GroupByKey<K, V>
    extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

  private final boolean fewKeys;

  private GroupByKey(boolean fewKeys) {
    this.fewKeys = fewKeys;
  }

  /**
   * Returns a {@code GroupByKey<K, V>} {@code PTransform}.
   *
   * @param <K> the type of the keys of the input and output {@code PCollection}s
   * @param <V> the type of the values of the input {@code PCollection} and the elements of the
   *     {@code Iterable}s in the output {@code PCollection}
   */
  public static <K, V> GroupByKey<K, V> create() {
    return new GroupByKey<>(false);
  }

  /**
   * Returns a {@code GroupByKey<K, V>} {@code PTransform} that assumes it will be grouping a small
   * number of keys.
   *
   * @param <K> the type of the keys of the input and output {@code PCollection}s
   * @param <V> the type of the values of the input {@code PCollection} and the elements of the
   *     {@code Iterable}s in the output {@code PCollection}
   */
  static <K, V> GroupByKey<K, V> createWithFewKeys() {
    return new GroupByKey<>(true);
  }

  /** Returns whether it groups just few keys. */
  public boolean fewKeys() {
    return fewKeys;
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
          "GroupByKey cannot be applied to non-bounded PCollection in the GlobalWindow without a"
              + " trigger. Use a Window.into or Window.triggering transform prior to GroupByKey.");
    }

    // Validate the window merge function.
    if (windowingStrategy.getWindowFn() instanceof InvalidWindows) {
      String cause = ((InvalidWindows<?>) windowingStrategy.getWindowFn()).getCause();
      throw new IllegalStateException(
          "GroupByKey must have a valid Window merge function.  " + "Invalid because: " + cause);
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
    WindowFn<?, ?> inputWindowFn = inputStrategy.getWindowFn();
    if (!inputWindowFn.isNonMerging()) {
      // Prevent merging windows again, without explicit user
      // involvement, e.g., by Window.into() or Window.remerge().
      inputWindowFn =
          new InvalidWindows<>(
              "WindowFn has already been consumed by previous GroupByKey", inputWindowFn);
    }

    // We also switch to the continuation trigger associated with the current trigger.
    return inputStrategy
        .withWindowFn(inputWindowFn)
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
      throw new IllegalStateException("the keyCoder of a GroupByKey must be deterministic", e);
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
  static <K, V> KvCoder<K, V> getInputKvCoder(Coder<KV<K, V>> inputCoder) {
    if (!(inputCoder instanceof KvCoder)) {
      throw new IllegalStateException("GroupByKey requires its input to use KvCoder");
    }
    return (KvCoder<K, V>) inputCoder;
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the {@code Coder} of the keys of the input to this transform, which is also used as the
   * {@code Coder} of the keys of the output of this transform.
   */
  public static <K, V> Coder<K> getKeyCoder(Coder<KV<K, V>> inputCoder) {
    return getInputKvCoder(inputCoder).getKeyCoder();
  }

  /** Returns the {@code Coder} of the values of the input to this transform. */
  public static <K, V> Coder<V> getInputValueCoder(Coder<KV<K, V>> inputCoder) {
    return getInputKvCoder(inputCoder).getValueCoder();
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
