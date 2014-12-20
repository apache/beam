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

import static com.google.cloud.dataflow.sdk.util.Structs.addLong;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.annotation.Nullable;

/**
 * Tests for AvroReaderFactory.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("rawtypes")
public class AvroReaderFactoryTest {
  private final String pathToAvroFile = "/path/to/file.avro";

  Reader<?> runTestCreateAvroReader(String filename, @Nullable Long start, @Nullable Long end,
      CloudObject encoding) throws Exception {
    CloudObject spec = CloudObject.forClassName("AvroSource");
    addString(spec, "filename", filename);
    if (start != null) {
      addLong(spec, "start_offset", start);
    }
    if (end != null) {
      addLong(spec, "end_offset", end);
    }

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(encoding);

    Reader<?> reader = ReaderFactory.create(
        PipelineOptionsFactory.create(), cloudSource, new BatchModeExecutionContext());
    return reader;
  }

  @Test
  public void testCreatePlainAvroByteReader() throws Exception {
    Coder<?> coder = WindowedValue.getValueOnlyCoder(BigEndianIntegerCoder.of());
    Reader<?> reader = runTestCreateAvroReader(pathToAvroFile, null, null, coder.asCloudObject());

    Assert.assertThat(reader, new IsInstanceOf(AvroByteReader.class));
    AvroByteReader avroReader = (AvroByteReader) reader;
    Assert.assertEquals(pathToAvroFile, avroReader.avroReader.filename);
    Assert.assertEquals(null, avroReader.avroReader.startPosition);
    Assert.assertEquals(null, avroReader.avroReader.endPosition);
    Assert.assertEquals(coder, avroReader.coder);
  }

  @Test
  public void testCreateRichAvroByteReader() throws Exception {
    Coder<?> coder = WindowedValue.getValueOnlyCoder(BigEndianIntegerCoder.of());
    Reader<?> reader = runTestCreateAvroReader(pathToAvroFile, 200L, 500L, coder.asCloudObject());

    Assert.assertThat(reader, new IsInstanceOf(AvroByteReader.class));
    AvroByteReader avroReader = (AvroByteReader) reader;
    Assert.assertEquals(pathToAvroFile, avroReader.avroReader.filename);
    Assert.assertEquals(200L, (long) avroReader.avroReader.startPosition);
    Assert.assertEquals(500L, (long) avroReader.avroReader.endPosition);
    Assert.assertEquals(coder, avroReader.coder);
  }

  @Test
  public void testCreateRichAvroReader() throws Exception {
    WindowedValue.WindowedValueCoder<?> coder =
        WindowedValue.getValueOnlyCoder(AvroCoder.of(Integer.class));
    Reader<?> reader = runTestCreateAvroReader(pathToAvroFile, 200L, 500L, coder.asCloudObject());

    Assert.assertThat(reader, new IsInstanceOf(AvroReader.class));
    AvroReader avroReader = (AvroReader) reader;
    Assert.assertEquals(pathToAvroFile, avroReader.filename);
    Assert.assertEquals(200L, (long) avroReader.startPosition);
    Assert.assertEquals(500L, (long) avroReader.endPosition);
    Assert.assertEquals(coder.getValueCoder(), avroReader.avroCoder);
  }
}
