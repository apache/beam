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

import com.google.api.services.dataflow.model.Source;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for UngroupedShuffleReaderFactory, GroupingShuffleReaderFactory, and
 * PartitioningShuffleReaderFactory.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class ShuffleReaderFactoryTest {
  <T extends NativeReader> T runTestCreateShuffleReader(
      byte[] shuffleReaderConfig,
      @Nullable String start,
      @Nullable String end,
      CloudObject encoding,
      BatchModeExecutionContext context,
      Class<T> shuffleReaderClass,
      String shuffleSourceAlias)
      throws Exception {
    CloudObject spec = CloudObject.forClassName(shuffleSourceAlias);
    addString(spec, "shuffle_reader_config", encodeBase64String(shuffleReaderConfig));
    if (start != null) {
      addString(spec, "start_shuffle_position", start);
    }
    if (end != null) {
      addString(spec, "end_shuffle_position", end);
    }

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(encoding);

    NativeReader<?> reader =
        ReaderRegistry.defaultRegistry()
            .create(cloudSource, PipelineOptionsFactory.create(), context, null);
    Assert.assertThat(reader, new IsInstanceOf(shuffleReaderClass));
    return (T) reader;
  }

  void runTestCreateUngroupedShuffleReader(
      byte[] shuffleReaderConfig, @Nullable String start, @Nullable String end, Coder<?> coder)
      throws Exception {
    UngroupedShuffleReader ungroupedShuffleReader =
        runTestCreateShuffleReader(
            shuffleReaderConfig,
            start,
            end,
            CloudObjects.asCloudObject(coder, /*sdkComponents=*/ null),
            BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "testStage"),
            UngroupedShuffleReader.class,
            "UngroupedShuffleSource");
    Assert.assertArrayEquals(shuffleReaderConfig, ungroupedShuffleReader.shuffleReaderConfig);
    Assert.assertEquals(start, ungroupedShuffleReader.startShufflePosition);
    Assert.assertEquals(end, ungroupedShuffleReader.stopShufflePosition);

    Assert.assertEquals(coder, ungroupedShuffleReader.coder);
  }

  void runTestCreateGroupingShuffleReader(
      byte[] shuffleReaderConfig,
      @Nullable String start,
      @Nullable String end,
      Coder<?> keyCoder,
      Coder<?> valueCoder)
      throws Exception {
    BatchModeExecutionContext context =
        BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "testStage");
    GroupingShuffleReader groupingShuffleReader =
        runTestCreateShuffleReader(
            shuffleReaderConfig,
            start,
            end,
            CloudObjects.asCloudObject(
                FullWindowedValueCoder.of(
                    KvCoder.of(keyCoder, IterableCoder.of(valueCoder)), IntervalWindowCoder.of()),
                /*sdkComponents=*/ null),
            context,
            GroupingShuffleReader.class,
            "GroupingShuffleSource");
    Assert.assertArrayEquals(shuffleReaderConfig, groupingShuffleReader.shuffleReaderConfig);
    Assert.assertEquals(start, groupingShuffleReader.startShufflePosition);
    Assert.assertEquals(end, groupingShuffleReader.stopShufflePosition);

    Assert.assertEquals(keyCoder, groupingShuffleReader.keyCoder);
    Assert.assertEquals(valueCoder, groupingShuffleReader.valueCoder);
    Assert.assertEquals(context, groupingShuffleReader.executionContext);
  }

  void runTestCreatePartitioningShuffleReader(
      byte[] shuffleReaderConfig,
      @Nullable String start,
      @Nullable String end,
      Coder<?> keyCoder,
      WindowedValueCoder<?> windowedValueCoder)
      throws Exception {
    PartitioningShuffleReader partitioningShuffleReader =
        runTestCreateShuffleReader(
            shuffleReaderConfig,
            start,
            end,
            CloudObjects.asCloudObject(
                FullWindowedValueCoder.of(
                    KvCoder.of(keyCoder, windowedValueCoder.getValueCoder()),
                    IntervalWindowCoder.of()),
                /*sdkComponents=*/ null),
            BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "testStage"),
            PartitioningShuffleReader.class,
            "PartitioningShuffleSource");
    Assert.assertArrayEquals(shuffleReaderConfig, partitioningShuffleReader.shuffleReaderConfig);
    Assert.assertEquals(start, partitioningShuffleReader.startShufflePosition);
    Assert.assertEquals(end, partitioningShuffleReader.stopShufflePosition);

    Assert.assertEquals(keyCoder, partitioningShuffleReader.keyCoder);
    Assert.assertEquals(windowedValueCoder, partitioningShuffleReader.windowedValueCoder);
  }

  @Test
  public void testCreatePlainUngroupedShuffleReader() throws Exception {
    runTestCreateUngroupedShuffleReader(new byte[] {(byte) 0xE1}, null, null, StringUtf8Coder.of());
  }

  @Test
  public void testCreateRichUngroupedShuffleReader() throws Exception {
    runTestCreateUngroupedShuffleReader(
        new byte[] {(byte) 0xE2}, "aaa", "zzz", BigEndianIntegerCoder.of());
  }

  @Test
  public void testCreatePlainGroupingShuffleReader() throws Exception {
    runTestCreateGroupingShuffleReader(
        new byte[] {(byte) 0xE1}, null, null, BigEndianIntegerCoder.of(), StringUtf8Coder.of());
  }

  @Test
  public void testCreateRichGroupingShuffleReader() throws Exception {
    runTestCreateGroupingShuffleReader(
        new byte[] {(byte) 0xE2},
        "aaa",
        "zzz",
        BigEndianIntegerCoder.of(),
        KvCoder.of(StringUtf8Coder.of(), VoidCoder.of()));
  }

  @Test
  public void testCreatePlainPartitioningShuffleReader() throws Exception {
    runTestCreatePartitioningShuffleReader(
        new byte[] {(byte) 0xE1},
        null,
        null,
        BigEndianIntegerCoder.of(),
        FullWindowedValueCoder.of(StringUtf8Coder.of(), IntervalWindow.getCoder()));
  }

  @Test
  public void testCreateRichPartitioningShuffleReader() throws Exception {
    runTestCreatePartitioningShuffleReader(
        new byte[] {(byte) 0xE2},
        "aaa",
        "zzz",
        BigEndianIntegerCoder.of(),
        FullWindowedValueCoder.of(
            KvCoder.of(StringUtf8Coder.of(), VoidCoder.of()), IntervalWindow.getCoder()));
  }
}
