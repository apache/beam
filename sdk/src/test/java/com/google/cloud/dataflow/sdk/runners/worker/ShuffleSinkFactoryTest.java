/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.api.client.util.Base64.encodeBase64String;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.FullWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for ShuffleSinkFactory.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("rawtypes")
public class ShuffleSinkFactoryTest {
  ShuffleSink runTestCreateShuffleSinkHelper(byte[] shuffleWriterConfig,
                                             String shuffleKind,
                                             CloudObject encoding,
                                             FullWindowedValueCoder<?> coder)
      throws Exception {
    CloudObject spec = CloudObject.forClassName("ShuffleSink");
    addString(spec, "shuffle_writer_config", encodeBase64String(shuffleWriterConfig));
    addString(spec, "shuffle_kind", shuffleKind);

    com.google.api.services.dataflow.model.Sink cloudSink =
        new com.google.api.services.dataflow.model.Sink();
    cloudSink.setSpec(spec);
    cloudSink.setCodec(encoding);

    Sink<?> sink = SinkFactory.create(PipelineOptionsFactory.create(),
                                      cloudSink,
                                      new BatchModeExecutionContext());
    Assert.assertThat(sink, new IsInstanceOf(ShuffleSink.class));
    ShuffleSink shuffleSink = (ShuffleSink) sink;
    Assert.assertArrayEquals(shuffleWriterConfig,
                             shuffleSink.shuffleWriterConfig);
    Assert.assertEquals(coder, shuffleSink.windowedElemCoder);
    return shuffleSink;
  }

  void runTestCreateUngroupingShuffleSink(byte[] shuffleWriterConfig,
                                          CloudObject encoding,
                                          FullWindowedValueCoder<?> coder)
      throws Exception {
    ShuffleSink shuffleSink = runTestCreateShuffleSinkHelper(
        shuffleWriterConfig, "ungrouped", encoding, coder);
    Assert.assertEquals(ShuffleSink.ShuffleKind.UNGROUPED,
                        shuffleSink.shuffleKind);
    Assert.assertFalse(shuffleSink.shardByKey);
    Assert.assertFalse(shuffleSink.groupValues);
    Assert.assertFalse(shuffleSink.sortValues);
    Assert.assertNull(shuffleSink.keyCoder);
    Assert.assertNull(shuffleSink.valueCoder);
    Assert.assertNull(shuffleSink.sortKeyCoder);
    Assert.assertNull(shuffleSink.sortValueCoder);
  }

  void runTestCreatePartitioningShuffleSink(byte[] shuffleWriterConfig,
                                            Coder<?> keyCoder,
                                            Coder<?> valueCoder)
      throws Exception {
    FullWindowedValueCoder<?> coder = (FullWindowedValueCoder<?>) WindowedValue.getFullCoder(
        KvCoder.of(keyCoder, valueCoder), IntervalWindow.getCoder());
    ShuffleSink shuffleSink = runTestCreateShuffleSinkHelper(
        shuffleWriterConfig, "partition_keys", coder.asCloudObject(), coder);
    Assert.assertEquals(ShuffleSink.ShuffleKind.PARTITION_KEYS,
                        shuffleSink.shuffleKind);
    Assert.assertTrue(shuffleSink.shardByKey);
    Assert.assertFalse(shuffleSink.groupValues);
    Assert.assertFalse(shuffleSink.sortValues);
    Assert.assertEquals(keyCoder, shuffleSink.keyCoder);
    Assert.assertEquals(valueCoder, shuffleSink.valueCoder);
    Assert.assertEquals(FullWindowedValueCoder.of(valueCoder,
                                                  IntervalWindow.getCoder()),
                        shuffleSink.windowedValueCoder);
    Assert.assertNull(shuffleSink.sortKeyCoder);
    Assert.assertNull(shuffleSink.sortValueCoder);
  }

  void runTestCreateGroupingShuffleSink(byte[] shuffleWriterConfig,
                                        Coder<?> keyCoder,
                                        Coder<?> valueCoder)
      throws Exception {
    FullWindowedValueCoder<?> coder = (FullWindowedValueCoder<?>) WindowedValue.getFullCoder(
        KvCoder.of(keyCoder, valueCoder), IntervalWindow.getCoder());
    ShuffleSink shuffleSink = runTestCreateShuffleSinkHelper(
        shuffleWriterConfig, "group_keys", coder.asCloudObject(), coder);
    Assert.assertEquals(ShuffleSink.ShuffleKind.GROUP_KEYS,
                        shuffleSink.shuffleKind);
    Assert.assertTrue(shuffleSink.shardByKey);
    Assert.assertTrue(shuffleSink.groupValues);
    Assert.assertFalse(shuffleSink.sortValues);
    Assert.assertEquals(keyCoder, shuffleSink.keyCoder);
    Assert.assertEquals(valueCoder, shuffleSink.valueCoder);
    Assert.assertNull(shuffleSink.windowedValueCoder);
    Assert.assertNull(shuffleSink.sortKeyCoder);
    Assert.assertNull(shuffleSink.sortValueCoder);
  }

  void runTestCreateGroupingSortingShuffleSink(byte[] shuffleWriterConfig,
                                               Coder<?> keyCoder,
                                               Coder<?> sortKeyCoder,
                                               Coder<?> sortValueCoder)
      throws Exception {
    FullWindowedValueCoder<?> coder = (FullWindowedValueCoder<?>) WindowedValue.getFullCoder(
        KvCoder.of(keyCoder, KvCoder.of(sortKeyCoder, sortValueCoder)),
        IntervalWindow.getCoder());
    ShuffleSink shuffleSink = runTestCreateShuffleSinkHelper(
        shuffleWriterConfig, "group_keys_and_sort_values", coder.asCloudObject(), coder);
    Assert.assertEquals(ShuffleSink.ShuffleKind.GROUP_KEYS_AND_SORT_VALUES,
                        shuffleSink.shuffleKind);
    Assert.assertTrue(shuffleSink.shardByKey);
    Assert.assertTrue(shuffleSink.groupValues);
    Assert.assertTrue(shuffleSink.sortValues);
    Assert.assertEquals(keyCoder, shuffleSink.keyCoder);
    Assert.assertEquals(KvCoder.of(sortKeyCoder, sortValueCoder),
                        shuffleSink.valueCoder);
    Assert.assertEquals(sortKeyCoder, shuffleSink.sortKeyCoder);
    Assert.assertEquals(sortValueCoder, shuffleSink.sortValueCoder);
    Assert.assertNull(shuffleSink.windowedValueCoder);
  }

  @Test
  public void testCreateUngroupingShuffleSink() throws Exception {
    FullWindowedValueCoder<?> coder = (FullWindowedValueCoder<?>) WindowedValue.getFullCoder(
        StringUtf8Coder.of(), IntervalWindow.getCoder());
    runTestCreateUngroupingShuffleSink(
        new byte[]{(byte) 0xE1},
        coder.asCloudObject(),
        coder);
  }

  @Test
  public void testCreatePartitionShuffleSink() throws Exception {
    runTestCreatePartitioningShuffleSink(
        new byte[]{(byte) 0xE2},
        BigEndianIntegerCoder.of(),
        StringUtf8Coder.of());
  }

  @Test
  public void testCreateGroupingShuffleSink() throws Exception {
    runTestCreateGroupingShuffleSink(
        new byte[]{(byte) 0xE2},
        BigEndianIntegerCoder.of(),
        WindowedValue.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder()));
  }

  @Test
  public void testCreateGroupingSortingShuffleSink() throws Exception {
    runTestCreateGroupingSortingShuffleSink(
        new byte[]{(byte) 0xE3},
        BigEndianIntegerCoder.of(),
        StringUtf8Coder.of(),
        VoidCoder.of());
  }
}
