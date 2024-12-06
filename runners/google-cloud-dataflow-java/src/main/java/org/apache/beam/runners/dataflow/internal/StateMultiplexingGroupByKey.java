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
package org.apache.beam.runners.dataflow.internal;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark.AfterWatermarkEarlyAndLate;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark.FromEndOfWindow;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.KeyedWindow;
import org.apache.beam.sdk.transforms.windowing.KeyedWindow.KeyedWindowFn;
import org.apache.beam.sdk.transforms.windowing.Never.NeverTrigger;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.common.base.Preconditions;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString.Output;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A GroupByKey implementation that multiplexes many small user keys over a fixed set of sharding
 * keys for reducing per key overhead.
 */
public class StateMultiplexingGroupByKey<K, V>
    extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

  /*
   * Keys larger than this threshold will not be multiplexed.
   */
  private static final int SMALL_KEY_BYTES_THRESHOLD = 4096;
  private final boolean fewKeys;
  private final int numShardingKeys;

  private StateMultiplexingGroupByKey(boolean fewKeys) {
    // :TODO plumb fewKeys to DataflowGroupByKey
    this.fewKeys = fewKeys;
    // :TODO Make this configurable
    this.numShardingKeys = 32 << 10;
  }

  /**
   * Returns a {@code GroupByKey<K, V>} {@code PTransform}.
   *
   * @param <K> the type of the keys of the input and output {@code PCollection}s
   * @param <V> the type of the values of the input {@code PCollection} and the elements of the
   *     {@code Iterable}s in the output {@code PCollection}
   */
  public static <K, V> StateMultiplexingGroupByKey<K, V> create(boolean fewKeys) {
    return new StateMultiplexingGroupByKey<>(fewKeys);
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
              "the GroupByKey requires its output coder to be %s but found %s.",
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

  @Override
  public PCollection<KV<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
    applicableTo(input);
    // Verify that the input Coder<KV<K, V>> is a KvCoder<K, V>, and that
    // the key coder is deterministic.
    Coder<K> keyCoder = getKeyCoder(input.getCoder());
    Coder<V> valueCoder = getInputValueCoder(input.getCoder());
    KvCoder<K, Iterable<V>> outputKvCoder = getOutputKvCoder(input.getCoder());

    try {
      keyCoder.verifyDeterministic();
    } catch (NonDeterministicException e) {
      throw new IllegalStateException("the keyCoder of a GroupByKey must be deterministic", e);
    }
    Preconditions.checkArgument(numShardingKeys > 0);
    final TupleTag<KV<ByteString, V>> largeKeys = new TupleTag<KV<ByteString, V>>() {};
    final TupleTag<KV<ByteString, V>> smallKeys = new TupleTag<KV<ByteString, V>>() {};
    WindowingStrategy<?, ?> originalWindowingStrategy = input.getWindowingStrategy();

    PCollectionTuple mapKeysToBytes =
        input.apply(
            "MapKeysToBytes",
            ParDo.of(
                    new DoFn<KV<K, V>, KV<ByteString, V>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        KV<K, V> kv = c.element();
                        Output output = ByteString.newOutput();
                        try {
                          keyCoder.encode(kv.getKey(), output);
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }

                        KV<ByteString, V> outputKV = KV.of(output.toByteString(), kv.getValue());
                        if (outputKV.getKey().size() <= SMALL_KEY_BYTES_THRESHOLD) {
                          c.output(smallKeys, outputKV);
                        } else {
                          c.output(largeKeys, outputKV);
                        }
                      }
                    })
                .withOutputTags(largeKeys, TupleTagList.of(smallKeys)));

    PCollection<KV<K, Iterable<V>>> largeKeyBranch =
        mapKeysToBytes
            .get(largeKeys)
            .setCoder(KvCoder.of(KeyedWindow.ByteStringCoder.of(), valueCoder))
            .apply(DataflowGroupByKey.create())
            .apply(
                "DecodeKey",
                MapElements.via(
                    new SimpleFunction<KV<ByteString, Iterable<V>>, KV<K, Iterable<V>>>() {
                      @Override
                      public KV<K, Iterable<V>> apply(KV<ByteString, Iterable<V>> kv) {
                        try {
                          return KV.of(keyCoder.decode(kv.getKey().newInput()), kv.getValue());
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    }))
            .setCoder(outputKvCoder);

    WindowFn<?, ?> windowFn = originalWindowingStrategy.getWindowFn();
    PCollection<KV<K, Iterable<V>>> smallKeyBranch =
        mapKeysToBytes
            .get(smallKeys)
            .apply(Window.into(new KeyedWindowFn<>(windowFn)))
            .apply(
                "MapKeys",
                MapElements.via(
                    new SimpleFunction<KV<ByteString, V>, KV<Integer, V>>() {
                      @Override
                      public KV<Integer, V> apply(KV<ByteString, V> value) {
                        return KV.of(value.getKey().hashCode() % numShardingKeys, value.getValue());
                      }
                    }))
            .apply(DataflowGroupByKey.create())
            .apply(
                "Restore Keys",
                ParDo.of(
                    new DoFn<KV<Integer, Iterable<V>>, KV<K, Iterable<V>>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c, BoundedWindow w, PaneInfo pane) {
                        ByteString key = ((KeyedWindow<?>) w).getKey();
                        try {

                          // is it correct to use the pane from Keyed window here?
                          c.outputWindowedValue(
                              KV.of(keyCoder.decode(key.newInput()), c.element().getValue()),
                              c.timestamp(),
                              Collections.singleton(((KeyedWindow<?>) w).getWindow()),
                              pane);
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    }))
            .setWindowingStrategyInternal(originalWindowingStrategy)
            .setCoder(outputKvCoder);
    return PCollectionList.of(Arrays.asList(smallKeyBranch, largeKeyBranch))
        .apply(Flatten.pCollections());
  }

  /**
   * Returns the {@code Coder} of the input to this transform, which should be a {@code KvCoder}.
   */
  @SuppressWarnings("unchecked")
  static <K, V> KvCoder<K, V> getInputKvCoder(Coder<?> inputCoder) {
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
    return StateMultiplexingGroupByKey.<K, V>getInputKvCoder(inputCoder).getKeyCoder();
  }

  /** Returns the {@code Coder} of the values of the input to this transform. */
  public static <K, V> Coder<V> getInputValueCoder(Coder<KV<K, V>> inputCoder) {
    return StateMultiplexingGroupByKey.<K, V>getInputKvCoder(inputCoder).getValueCoder();
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
