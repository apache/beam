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
package org.apache.beam.runners.dataflow.worker;

import static com.google.api.client.util.Base64.encodeBase64String;
import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ShuffleSinkFactory. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class ShuffleSinkFactoryTest {

  private ShuffleSink runTestCreateShuffleSinkHelper(
      byte[] shuffleWriterConfig,
      String shuffleKind,
      Coder<?> deserializedCoder,
      FullWindowedValueCoder<?> coder)
      throws Exception {
    CloudObject spec = CloudObject.forClassName("ShuffleSink");
    addString(spec, "shuffle_writer_config", encodeBase64String(shuffleWriterConfig));
    addString(spec, "shuffle_kind", shuffleKind);

    PipelineOptions options = PipelineOptionsFactory.create();

    ShuffleSinkFactory factory = new ShuffleSinkFactory();
    Sink<?> sink =
        factory.create(
            spec,
            deserializedCoder,
            options,
            BatchModeExecutionContext.forTesting(options, "testStage"),
            TestOperationContext.create());
    assertThat(sink, new IsInstanceOf(ShuffleSink.class));
    ShuffleSink shuffleSink = (ShuffleSink) sink;
    Assert.assertArrayEquals(shuffleWriterConfig, shuffleSink.shuffleWriterConfig);
    Assert.assertEquals(coder, shuffleSink.windowedElemCoder);
    return shuffleSink;
  }

  void runTestCreateUngroupingShuffleSink(
      byte[] shuffleWriterConfig, Coder<?> deserializedCoder, FullWindowedValueCoder<?> coder)
      throws Exception {
    ShuffleSink shuffleSink =
        runTestCreateShuffleSinkHelper(shuffleWriterConfig, "ungrouped", deserializedCoder, coder);
    Assert.assertEquals(ShuffleSink.ShuffleKind.UNGROUPED, shuffleSink.shuffleKind);
    Assert.assertFalse(shuffleSink.shardByKey);
    Assert.assertFalse(shuffleSink.groupValues);
    Assert.assertFalse(shuffleSink.sortValues);
    Assert.assertNull(shuffleSink.keyCoder);
    Assert.assertNull(shuffleSink.valueCoder);
    Assert.assertNull(shuffleSink.sortKeyCoder);
    Assert.assertNull(shuffleSink.sortValueCoder);
  }

  void runTestCreatePartitioningShuffleSink(
      byte[] shuffleWriterConfig, Coder<?> keyCoder, Coder<?> valueCoder) throws Exception {
    FullWindowedValueCoder<?> coder =
        WindowedValue.getFullCoder(KvCoder.of(keyCoder, valueCoder), IntervalWindow.getCoder());
    ShuffleSink shuffleSink =
        runTestCreateShuffleSinkHelper(shuffleWriterConfig, "partition_keys", coder, coder);
    Assert.assertEquals(ShuffleSink.ShuffleKind.PARTITION_KEYS, shuffleSink.shuffleKind);
    Assert.assertTrue(shuffleSink.shardByKey);
    Assert.assertFalse(shuffleSink.groupValues);
    Assert.assertFalse(shuffleSink.sortValues);
    Assert.assertEquals(keyCoder, shuffleSink.keyCoder);
    Assert.assertEquals(valueCoder, shuffleSink.valueCoder);
    Assert.assertEquals(
        FullWindowedValueCoder.of(valueCoder, IntervalWindow.getCoder()),
        shuffleSink.windowedValueCoder);
    Assert.assertNull(shuffleSink.sortKeyCoder);
    Assert.assertNull(shuffleSink.sortValueCoder);
  }

  void runTestCreateGroupingShuffleSink(
      byte[] shuffleWriterConfig, Coder<?> keyCoder, Coder<?> valueCoder) throws Exception {
    FullWindowedValueCoder<?> coder =
        WindowedValue.getFullCoder(KvCoder.of(keyCoder, valueCoder), IntervalWindow.getCoder());
    ShuffleSink shuffleSink =
        runTestCreateShuffleSinkHelper(shuffleWriterConfig, "group_keys", coder, coder);
    Assert.assertEquals(ShuffleSink.ShuffleKind.GROUP_KEYS, shuffleSink.shuffleKind);
    Assert.assertTrue(shuffleSink.shardByKey);
    Assert.assertTrue(shuffleSink.groupValues);
    Assert.assertFalse(shuffleSink.sortValues);
    Assert.assertEquals(keyCoder, shuffleSink.keyCoder);
    Assert.assertEquals(valueCoder, shuffleSink.valueCoder);
    Assert.assertNull(shuffleSink.windowedValueCoder);
    Assert.assertNull(shuffleSink.sortKeyCoder);
    Assert.assertNull(shuffleSink.sortValueCoder);
  }

  void runTestCreateGroupingSortingShuffleSink(
      byte[] shuffleWriterConfig, Coder<?> keyCoder, Coder<?> sortKeyCoder, Coder<?> sortValueCoder)
      throws Exception {
    FullWindowedValueCoder<?> coder =
        WindowedValue.getFullCoder(
            KvCoder.of(keyCoder, KvCoder.of(sortKeyCoder, sortValueCoder)),
            IntervalWindow.getCoder());
    ShuffleSink shuffleSink =
        runTestCreateShuffleSinkHelper(
            shuffleWriterConfig, "group_keys_and_sort_values", coder, coder);
    Assert.assertEquals(
        ShuffleSink.ShuffleKind.GROUP_KEYS_AND_SORT_VALUES, shuffleSink.shuffleKind);
    Assert.assertTrue(shuffleSink.shardByKey);
    Assert.assertTrue(shuffleSink.groupValues);
    Assert.assertTrue(shuffleSink.sortValues);
    Assert.assertEquals(keyCoder, shuffleSink.keyCoder);
    Assert.assertEquals(KvCoder.of(sortKeyCoder, sortValueCoder), shuffleSink.valueCoder);
    Assert.assertEquals(sortKeyCoder, shuffleSink.sortKeyCoder);
    Assert.assertEquals(sortValueCoder, shuffleSink.sortValueCoder);
    Assert.assertNull(shuffleSink.windowedValueCoder);
  }

  @Test
  public void testCreateUngroupingShuffleSink() throws Exception {
    FullWindowedValueCoder<?> coder =
        WindowedValue.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());
    runTestCreateUngroupingShuffleSink(new byte[] {(byte) 0xE1}, coder, coder);
  }

  @Test
  public void testCreatePartitionShuffleSink() throws Exception {
    runTestCreatePartitioningShuffleSink(
        new byte[] {(byte) 0xE2}, BigEndianIntegerCoder.of(), StringUtf8Coder.of());
  }

  @Test
  public void testCreateGroupingShuffleSink() throws Exception {
    runTestCreateGroupingShuffleSink(
        new byte[] {(byte) 0xE2},
        BigEndianIntegerCoder.of(),
        WindowedValue.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder()));
  }

  @Test
  public void testCreateGroupingSortingShuffleSink() throws Exception {
    runTestCreateGroupingSortingShuffleSink(
        new byte[] {(byte) 0xE3}, BigEndianIntegerCoder.of(), StringUtf8Coder.of(), VoidCoder.of());
  }
}
