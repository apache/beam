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

package org.apache.beam.runners.gearpump.translators.io;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.gearpump.DefaultMessage;
import org.apache.gearpump.Message;
import org.apache.gearpump.streaming.source.Watermark;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link GearpumpSource}. */
public class GearpumpSourceTest {
  private static final List<TimestampedValue<String>> TEST_VALUES =
      Lists.newArrayList(
          TimestampedValue.of("a", BoundedWindow.TIMESTAMP_MIN_VALUE),
          TimestampedValue.of("b", new org.joda.time.Instant(0)),
          TimestampedValue.of("c", new org.joda.time.Instant(53)),
          TimestampedValue.of("d", BoundedWindow.TIMESTAMP_MAX_VALUE)
      );

  private static class SourceForTest<T> extends GearpumpSource<T> {
    private ValuesSource<T> valuesSource;

    SourceForTest(PipelineOptions options, ValuesSource<T> valuesSource) {
      super(options);
      this.valuesSource = valuesSource;
    }

    @Override
    protected Source.Reader<T> createReader(PipelineOptions options) throws IOException {
      return this.valuesSource.createReader(options, null);
    }
  }

  @Test
  public void testGearpumpSource() {
    GearpumpPipelineOptions options =
        PipelineOptionsFactory.create().as(GearpumpPipelineOptions.class);
    ValuesSource<TimestampedValue<String>> valuesSource =
        new ValuesSource<>(
            TEST_VALUES, TimestampedValue.TimestampedValueCoder.of(StringUtf8Coder.of()));
    SourceForTest<TimestampedValue<String>> sourceForTest =
        new SourceForTest<>(options, valuesSource);
    sourceForTest.open(null, Instant.EPOCH);

    for (int i = 0; i < TEST_VALUES.size(); i++) {
      TimestampedValue<String> value = TEST_VALUES.get(i);

      // Check the watermark first since the Source will advance when it's opened
      if (i < TEST_VALUES.size() - 1) {
        Instant expectedWaterMark = TranslatorUtils.jodaTimeToJava8Time(value.getTimestamp());
        Assert.assertEquals(expectedWaterMark, sourceForTest.getWatermark());
      } else {
        Assert.assertEquals(Watermark.MAX(), sourceForTest.getWatermark());
      }

      Message expectedMsg =
          new DefaultMessage(
              WindowedValue.timestampedValueInGlobalWindow(value, value.getTimestamp()),
              value.getTimestamp().getMillis());
      Message message = sourceForTest.read();
      Assert.assertEquals(expectedMsg, message);
    }

    Assert.assertNull(sourceForTest.read());
    Assert.assertEquals(Watermark.MAX(), sourceForTest.getWatermark());
  }
}
