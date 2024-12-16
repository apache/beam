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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.internal.KeyedWindow.KeyedWindowFn;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.ByteStringCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

/**
 * A GroupByKey implementation that multiplexes many small user keys over a fixed set of sharding
 * keys for reducing per key overhead.
 */
public class StateMultiplexingGroupByKey<K, V>
    extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

  public static final String EXPERIMENT_ENABLE_GBK_STATE_MULTIPLEXING =
      "enable_gbk_state_multiplexing";
  private static final String EXPERIMENT_NUM_VIRTUAL_KEYS = "gbk_state_multiplexing_num_keys";
  private static final String EXPERIMENT_SMALL_KEY_BYTES_THRESHOLD =
      "gbk_state_multiplexing_small_key_bytes";

  /*
   * Keys larger than this threshold will not be multiplexed.
   */
  private static final int DEFAULT_SMALL_KEY_BYTES_THRESHOLD = 4096;
  private static final int DEFAULT_NUM_VIRTUAL_KEYS = 32 << 10;
  private final boolean fewKeys;
  private final int numVirtualKeys;
  private final int smallKeyBytesThreshold;

  private StateMultiplexingGroupByKey(DataflowPipelineOptions options, boolean fewKeys) {
    this.fewKeys = fewKeys;
    this.numVirtualKeys =
        getExperimentValue(options, EXPERIMENT_NUM_VIRTUAL_KEYS, DEFAULT_NUM_VIRTUAL_KEYS);
    this.smallKeyBytesThreshold =
        getExperimentValue(
            options, EXPERIMENT_SMALL_KEY_BYTES_THRESHOLD, DEFAULT_SMALL_KEY_BYTES_THRESHOLD);
  }

  private static int getExperimentValue(
      DataflowPipelineOptions options, String experiment, int defaultValue) {
    return DataflowRunner.getExperimentValue(options, experiment)
        .map(Integer::parseInt)
        .orElse(defaultValue);
  }

  /**
   * Returns a {@code StateMultiplexingGroupByKey<K, V>} {@code PTransform}.
   *
   * @param <K> the type of the keys of the input and output {@code PCollection}s
   * @param <V> the type of the values of the input {@code PCollection} and the elements of the
   *     {@code Iterable}s in the output {@code PCollection}
   */
  public static <K, V> StateMultiplexingGroupByKey<K, V> create(
      DataflowPipelineOptions options, boolean fewKeys) {
    return new StateMultiplexingGroupByKey<>(options, fewKeys);
  }

  @Override
  public PCollection<KV<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
    DataflowGroupByKey.applicableTo(input);
    // Verify that the input Coder<KV<K, V>> is a KvCoder<K, V>, and that
    // the key coder is deterministic.
    Coder<K> keyCoder = DataflowGroupByKey.getKeyCoder(input.getCoder());
    try {
      keyCoder.verifyDeterministic();
    } catch (NonDeterministicException e) {
      throw new IllegalStateException("the keyCoder of a GroupByKey must be deterministic", e);
    }
    Coder<V> valueCoder = DataflowGroupByKey.getInputValueCoder(input.getCoder());
    KvCoder<K, Iterable<V>> outputKvCoder = DataflowGroupByKey.getOutputKvCoder(input.getCoder());

    Preconditions.checkArgument(numVirtualKeys > 0);
    final TupleTag<KV<ByteString, V>> largeKeys = new TupleTag<KV<ByteString, V>>() {};
    final TupleTag<KV<ByteString, V>> smallKeys = new TupleTag<KV<ByteString, V>>() {};
    WindowingStrategy<?, ?> originalWindowingStrategy = input.getWindowingStrategy();
    WindowFn<?, ?> originalWindowFn = originalWindowingStrategy.getWindowFn();

    PCollectionTuple mapKeysToBytes =
        input.apply(
            "MapKeysToBytes",
            ParDo.of(
                    new DoFn<KV<K, V>, KV<ByteString, V>>() {
                      transient ByteStringOutputStream byteStringOutputStream =
                          new ByteStringOutputStream();

                      @Setup
                      public void setup() {
                        byteStringOutputStream = new ByteStringOutputStream();
                      }

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        KV<K, V> kv = c.element();
                        if (kv.getKey() == null) {
                          // Combine.globally treats null keys specially
                          // so don't multiplex them.
                          c.output(largeKeys, KV.of(null, kv.getValue()));
                          return;
                        }
                        try {
                          // clear output stream
                          byteStringOutputStream.toByteStringAndReset();
                          keyCoder.encode(kv.getKey(), byteStringOutputStream);
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }

                        ByteString keyBytes = byteStringOutputStream.toByteStringAndReset();
                        KV<ByteString, V> outputKV = KV.of(keyBytes, kv.getValue());
                        if (keyBytes.size() <= smallKeyBytesThreshold) {
                          c.output(smallKeys, outputKV);
                        } else {
                          c.output(largeKeys, outputKV);
                        }
                      }
                    })
                .withOutputTags(largeKeys, TupleTagList.of(smallKeys)));

    // Pass large keys as it is through DataflowGroupByKey
    PCollection<KV<K, Iterable<V>>> largeKeyBranch =
        mapKeysToBytes
            .get(largeKeys)
            .setCoder(KvCoder.of(ByteStringCoder.of(), valueCoder))
            .apply(
                fewKeys
                    ? DataflowGroupByKey.<ByteString, V>createWithFewKeys()
                    : DataflowGroupByKey.<ByteString, V>create())
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

    // Multiplex small keys over `numShardingKeys` virtual keys.
    // Original user keys are sent as part of windows.
    // After GroupByKey the original keys are restored from windows.
    PCollection<KV<K, Iterable<V>>> smallKeyBranch =
        mapKeysToBytes
            .get(smallKeys)
            .apply(Window.into(new KeyedWindowFn<>(originalWindowFn)))
            .apply(
                "MapKeysToVirtualKeys",
                MapElements.via(
                    new SimpleFunction<KV<ByteString, V>, KV<Integer, V>>() {
                      @Override
                      public KV<Integer, V> apply(KV<ByteString, V> value) {
                        return KV.of(value.getKey().hashCode() % numVirtualKeys, value.getValue());
                      }
                    }))
            .apply(
                fewKeys
                    ? DataflowGroupByKey.<Integer, V>createWithFewKeys()
                    : DataflowGroupByKey.<Integer, V>create())
            .apply(
                "RestoreOriginalKeys",
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

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    if (fewKeys) {
      builder.add(DisplayData.item("fewKeys", true).withLabel("Has Few Keys"));
    }
    builder.add(DisplayData.item("numVirtualKeys", numVirtualKeys).withLabel("Num Virtual Keys"));
    builder.add(
        DisplayData.item("smallKeyBytesThreshold", smallKeyBytesThreshold)
            .withLabel("Small Key Bytes Threshold"));
  }
}
