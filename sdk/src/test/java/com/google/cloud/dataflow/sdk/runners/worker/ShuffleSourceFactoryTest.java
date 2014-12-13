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
import com.google.cloud.dataflow.sdk.util.common.worker.Source;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.annotation.Nullable;

/**
 * Tests for UngroupedShuffleSourceFactory, GroupingShuffleSourceFactory,
 * and PartitioningShuffleSourceFactory.
 */
@RunWith(JUnit4.class)
public class ShuffleSourceFactoryTest {
  <T extends Source>
  T runTestCreateShuffleSource(byte[] shuffleReaderConfig,
                               @Nullable String start,
                               @Nullable String end,
                               CloudObject encoding,
                               BatchModeExecutionContext context,
                               Class<T> shuffleSourceClass)
      throws Exception {
    CloudObject spec = CloudObject.forClassName(shuffleSourceClass.getSimpleName());
    addString(spec, "shuffle_reader_config", encodeBase64String(shuffleReaderConfig));
    if (start != null) {
      addString(spec, "start_shuffle_position", start);
    }
    if (end != null) {
      addString(spec, "end_shuffle_position", end);
    }

    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(encoding);

    Source<?> source = SourceFactory.create(
        PipelineOptionsFactory.create(), cloudSource, context);
    Assert.assertThat(source, new IsInstanceOf(shuffleSourceClass));
    T shuffleSource = (T) source;
    return shuffleSource;
  }

  void runTestCreateUngroupedShuffleSource(byte[] shuffleReaderConfig,
                                           @Nullable String start,
                                           @Nullable String end,
                                           CloudObject encoding,
                                           Coder<?> coder) throws Exception {
    UngroupedShuffleSource shuffleSource =
        runTestCreateShuffleSource(shuffleReaderConfig,
                                   start,
                                   end,
                                   encoding,
                                   new BatchModeExecutionContext(),
                                   UngroupedShuffleSource.class);
    Assert.assertArrayEquals(shuffleReaderConfig,
                             shuffleSource.shuffleReaderConfig);
    Assert.assertEquals(start, shuffleSource.startShufflePosition);
    Assert.assertEquals(end, shuffleSource.stopShufflePosition);

    Assert.assertEquals(coder, shuffleSource.coder);
  }

  void runTestCreateGroupingShuffleSource(byte[] shuffleReaderConfig,
                                          @Nullable String start,
                                          @Nullable String end,
                                          CloudObject encoding,
                                          Coder<?> keyCoder,
                                          Coder<?> valueCoder) throws Exception {
    BatchModeExecutionContext context = new BatchModeExecutionContext();
    GroupingShuffleSource shuffleSource =
        runTestCreateShuffleSource(shuffleReaderConfig,
                                   start,
                                   end,
                                   encoding,
                                   context,
                                   GroupingShuffleSource.class);
    Assert.assertArrayEquals(shuffleReaderConfig,
                             shuffleSource.shuffleReaderConfig);
    Assert.assertEquals(start, shuffleSource.startShufflePosition);
    Assert.assertEquals(end, shuffleSource.stopShufflePosition);

    Assert.assertEquals(keyCoder, shuffleSource.keyCoder);
    Assert.assertEquals(valueCoder, shuffleSource.valueCoder);
    Assert.assertEquals(context, shuffleSource.executionContext);
  }

  void runTestCreatePartitioningShuffleSource(byte[] shuffleReaderConfig,
                                              @Nullable String start,
                                              @Nullable String end,
                                              CloudObject encoding,
                                              Coder<?> keyCoder,
                                              Coder<?> windowedValueCoder) throws Exception {
    PartitioningShuffleSource shuffleSource =
        runTestCreateShuffleSource(shuffleReaderConfig,
                                   start,
                                   end,
                                   encoding,
                                   new BatchModeExecutionContext(),
                                   PartitioningShuffleSource.class);
    Assert.assertArrayEquals(shuffleReaderConfig,
                             shuffleSource.shuffleReaderConfig);
    Assert.assertEquals(start, shuffleSource.startShufflePosition);
    Assert.assertEquals(end, shuffleSource.stopShufflePosition);

    Assert.assertEquals(keyCoder, shuffleSource.keyCoder);
    Assert.assertEquals(windowedValueCoder, shuffleSource.windowedValueCoder);
  }

  @Test
  public void testCreatePlainUngroupedShuffleSource() throws Exception {
    runTestCreateUngroupedShuffleSource(
        new byte[]{(byte) 0xE1}, null, null,
        makeCloudEncoding("StringUtf8Coder"),
        StringUtf8Coder.of());
  }

  @Test
  public void testCreateRichUngroupedShuffleSource() throws Exception {
    runTestCreateUngroupedShuffleSource(
        new byte[]{(byte) 0xE2}, "aaa", "zzz",
        makeCloudEncoding("BigEndianIntegerCoder"),
        BigEndianIntegerCoder.of());
  }

  @Test
  public void testCreatePlainGroupingShuffleSource() throws Exception {
    runTestCreateGroupingShuffleSource(
        new byte[]{(byte) 0xE1}, null, null,
        makeCloudEncoding(
            FullWindowedValueCoder.class.getName(),
            makeCloudEncoding(
                "KvCoder",
                makeCloudEncoding("BigEndianIntegerCoder"),
                makeCloudEncoding(
                    "IterableCoder",
                    makeCloudEncoding("StringUtf8Coder"))),
            IntervalWindow.getCoder().asCloudObject()),
        BigEndianIntegerCoder.of(),
        StringUtf8Coder.of());
  }

  @Test
  public void testCreateRichGroupingShuffleSource() throws Exception {
    runTestCreateGroupingShuffleSource(
        new byte[]{(byte) 0xE2}, "aaa", "zzz",
        makeCloudEncoding(
            FullWindowedValueCoder.class.getName(),
            makeCloudEncoding(
                "KvCoder",
                makeCloudEncoding("BigEndianIntegerCoder"),
                makeCloudEncoding(
                    "IterableCoder",
                    makeCloudEncoding(
                        "KvCoder",
                        makeCloudEncoding("StringUtf8Coder"),
                        makeCloudEncoding("VoidCoder")))),
            IntervalWindow.getCoder().asCloudObject()),
        BigEndianIntegerCoder.of(),
        KvCoder.of(StringUtf8Coder.of(), VoidCoder.of()));
  }

  @Test
  public void testCreatePlainPartitioningShuffleSource() throws Exception {
    runTestCreatePartitioningShuffleSource(
        new byte[]{(byte) 0xE1}, null, null,
        makeCloudEncoding(
            FullWindowedValueCoder.class.getName(),
            makeCloudEncoding(
                "KvCoder",
                makeCloudEncoding("BigEndianIntegerCoder"),
                makeCloudEncoding("StringUtf8Coder")),
            IntervalWindow.getCoder().asCloudObject()),
        BigEndianIntegerCoder.of(),
        FullWindowedValueCoder.of(StringUtf8Coder.of(), IntervalWindow.getCoder()));
  }

  @Test
  public void testCreateRichPartitioningShuffleSource() throws Exception {
    runTestCreatePartitioningShuffleSource(
        new byte[]{(byte) 0xE2}, "aaa", "zzz",
        makeCloudEncoding(
            FullWindowedValueCoder.class.getName(),
            makeCloudEncoding(
                "KvCoder",
                makeCloudEncoding("BigEndianIntegerCoder"),
                makeCloudEncoding(
                    "KvCoder",
                    makeCloudEncoding("StringUtf8Coder"),
                    makeCloudEncoding("VoidCoder"))),
            IntervalWindow.getCoder().asCloudObject()),
        BigEndianIntegerCoder.of(),
        FullWindowedValueCoder.of(KvCoder.of(StringUtf8Coder.of(), VoidCoder.of()),
                                  IntervalWindow.getCoder()));
  }
}
