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
import static com.google.cloud.dataflow.sdk.util.CoderUtils.makeCloudEncoding;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.WindowedValue.FullWindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.annotation.Nullable;

/**
 * Tests for UngroupedShuffleReaderFactory, GroupingShuffleReaderFactory,
 * and PartitioningShuffleReaderFactory.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class ShuffleReaderFactoryTest {
  <T extends Reader> T runTestCreateShuffleReader(byte[] shuffleReaderConfig,
      @Nullable String start, @Nullable String end, CloudObject encoding,
      BatchModeExecutionContext context, Class<?> shuffleReaderClass, String shuffleSourceAlias)
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

    Reader<?> reader = ReaderFactory.create(PipelineOptionsFactory.create(), cloudSource, context);
    Assert.assertThat(reader, new IsInstanceOf(shuffleReaderClass));
    T shuffleSource = (T) reader;
    return shuffleSource;
  }

  void runTestCreateUngroupedShuffleReader(byte[] shuffleReaderConfig, @Nullable String start,
      @Nullable String end, CloudObject encoding, Coder<?> coder) throws Exception {
    UngroupedShuffleReader ungroupedShuffleReader = runTestCreateShuffleReader(shuffleReaderConfig,
        start, end, encoding, new BatchModeExecutionContext(), UngroupedShuffleReader.class,
        "UngroupedShuffleSource");
    Assert.assertArrayEquals(shuffleReaderConfig, ungroupedShuffleReader.shuffleReaderConfig);
    Assert.assertEquals(start, ungroupedShuffleReader.startShufflePosition);
    Assert.assertEquals(end, ungroupedShuffleReader.stopShufflePosition);

    Assert.assertEquals(coder, ungroupedShuffleReader.coder);
  }

  void runTestCreateGroupingShuffleReader(byte[] shuffleReaderConfig, @Nullable String start,
      @Nullable String end, CloudObject encoding, Coder<?> keyCoder, Coder<?> valueCoder)
      throws Exception {
    BatchModeExecutionContext context = new BatchModeExecutionContext();
    GroupingShuffleReader groupingShuffleReader = runTestCreateShuffleReader(
        shuffleReaderConfig, start, end, encoding, context, GroupingShuffleReader.class,
        "GroupingShuffleSource");
    Assert.assertArrayEquals(shuffleReaderConfig, groupingShuffleReader.shuffleReaderConfig);
    Assert.assertEquals(start, groupingShuffleReader.startShufflePosition);
    Assert.assertEquals(end, groupingShuffleReader.stopShufflePosition);

    Assert.assertEquals(keyCoder, groupingShuffleReader.keyCoder);
    Assert.assertEquals(valueCoder, groupingShuffleReader.valueCoder);
    Assert.assertEquals(context, groupingShuffleReader.executionContext);
  }

  void runTestCreatePartitioningShuffleReader(byte[] shuffleReaderConfig, @Nullable String start,
      @Nullable String end, CloudObject encoding, Coder<?> keyCoder, Coder<?> windowedValueCoder)
      throws Exception {
    PartitioningShuffleReader partitioningShuffleReader =
        runTestCreateShuffleReader(shuffleReaderConfig, start, end, encoding,
            new BatchModeExecutionContext(), PartitioningShuffleReader.class,
            "PartitioningShuffleSource");
    Assert.assertArrayEquals(shuffleReaderConfig, partitioningShuffleReader.shuffleReaderConfig);
    Assert.assertEquals(start, partitioningShuffleReader.startShufflePosition);
    Assert.assertEquals(end, partitioningShuffleReader.stopShufflePosition);

    Assert.assertEquals(keyCoder, partitioningShuffleReader.keyCoder);
    Assert.assertEquals(windowedValueCoder, partitioningShuffleReader.windowedValueCoder);
  }

  @Test
  public void testCreatePlainUngroupedShuffleReader() throws Exception {
    runTestCreateUngroupedShuffleReader(new byte[] {(byte) 0xE1}, null, null,
        makeCloudEncoding("StringUtf8Coder"), StringUtf8Coder.of());
  }

  @Test
  public void testCreateRichUngroupedShuffleReader() throws Exception {
    runTestCreateUngroupedShuffleReader(new byte[] {(byte) 0xE2}, "aaa", "zzz",
        makeCloudEncoding("BigEndianIntegerCoder"), BigEndianIntegerCoder.of());
  }

  @Test
  public void testCreatePlainGroupingShuffleReader() throws Exception {
    runTestCreateGroupingShuffleReader(
        new byte[] {(byte) 0xE1}, null, null,
        makeCloudEncoding(
            FullWindowedValueCoder.class.getName(),
            makeCloudEncoding("KvCoder", makeCloudEncoding("BigEndianIntegerCoder"),
                makeCloudEncoding("IterableCoder", makeCloudEncoding("StringUtf8Coder"))),
            IntervalWindow.getCoder().asCloudObject()),
        BigEndianIntegerCoder.of(), StringUtf8Coder.of());
  }

  @Test
  public void testCreateRichGroupingShuffleReader() throws Exception {
    runTestCreateGroupingShuffleReader(
        new byte[] {(byte) 0xE2}, "aaa", "zzz",
        makeCloudEncoding(
            FullWindowedValueCoder.class.getName(),
            makeCloudEncoding(
                "KvCoder", makeCloudEncoding("BigEndianIntegerCoder"),
                makeCloudEncoding(
                    "IterableCoder",
                    makeCloudEncoding("KvCoder", makeCloudEncoding("StringUtf8Coder"),
                        makeCloudEncoding("VoidCoder")))),
            IntervalWindow.getCoder().asCloudObject()),
        BigEndianIntegerCoder.of(), KvCoder.of(StringUtf8Coder.of(), VoidCoder.of()));
  }

  @Test
  public void testCreatePlainPartitioningShuffleReader() throws Exception {
    runTestCreatePartitioningShuffleReader(
        new byte[] {(byte) 0xE1}, null, null,
        makeCloudEncoding(
            FullWindowedValueCoder.class.getName(),
            makeCloudEncoding("KvCoder", makeCloudEncoding("BigEndianIntegerCoder"),
                makeCloudEncoding("StringUtf8Coder")),
            IntervalWindow.getCoder().asCloudObject()),
        BigEndianIntegerCoder.of(),
        FullWindowedValueCoder.of(StringUtf8Coder.of(), IntervalWindow.getCoder()));
  }

  @Test
  public void testCreateRichPartitioningShuffleReader() throws Exception {
    runTestCreatePartitioningShuffleReader(
        new byte[] {(byte) 0xE2}, "aaa", "zzz",
        makeCloudEncoding(
            FullWindowedValueCoder.class.getName(),
            makeCloudEncoding(
                "KvCoder", makeCloudEncoding("BigEndianIntegerCoder"),
                makeCloudEncoding("KvCoder", makeCloudEncoding("StringUtf8Coder"),
                    makeCloudEncoding("VoidCoder"))),
            IntervalWindow.getCoder().asCloudObject()),
        BigEndianIntegerCoder.of(),
        FullWindowedValueCoder.of(
            KvCoder.of(StringUtf8Coder.of(), VoidCoder.of()), IntervalWindow.getCoder()));
  }
}
