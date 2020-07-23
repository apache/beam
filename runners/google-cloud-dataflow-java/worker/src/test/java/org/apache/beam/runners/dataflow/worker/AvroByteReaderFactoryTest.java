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

import static org.apache.beam.runners.dataflow.util.Structs.addLong;
import static org.apache.beam.runners.dataflow.util.Structs.addString;

import com.google.api.services.dataflow.model.Source;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link AvroByteReaderFactory}. */
@RunWith(JUnit4.class)
@SuppressWarnings("rawtypes")
public class AvroByteReaderFactoryTest {
  private final String pathToAvroFile = "/path/to/file.avro";

  NativeReader<?> runTestCreateAvroReader(
      String filename, @Nullable Long start, @Nullable Long end, CloudObject encoding)
      throws Exception {
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

    NativeReader<?> reader =
        ReaderRegistry.defaultRegistry()
            .create(
                cloudSource,
                PipelineOptionsFactory.create(),
                null, // ExecutionContext
                null);
    return reader;
  }

  @Test
  public void testCreatePlainAvroByteReader() throws Exception {
    Coder<?> coder =
        WindowedValue.getFullCoder(BigEndianIntegerCoder.of(), GlobalWindow.Coder.INSTANCE);
    NativeReader<?> reader =
        runTestCreateAvroReader(
            pathToAvroFile, null, null, CloudObjects.asCloudObject(coder, /*sdkComponents=*/ null));

    Assert.assertThat(reader, new IsInstanceOf(AvroByteReader.class));
    AvroByteReader avroReader = (AvroByteReader) reader;
    Assert.assertEquals(pathToAvroFile, avroReader.avroSource.getFileOrPatternSpec());
    Assert.assertEquals(0L, avroReader.startPosition);
    Assert.assertEquals(Long.MAX_VALUE, avroReader.endPosition);
    Assert.assertEquals(coder, avroReader.coder);
  }

  @Test
  public void testCreateRichAvroByteReader() throws Exception {
    Coder<?> coder =
        WindowedValue.getFullCoder(BigEndianIntegerCoder.of(), GlobalWindow.Coder.INSTANCE);
    NativeReader<?> reader =
        runTestCreateAvroReader(
            pathToAvroFile, 200L, 500L, CloudObjects.asCloudObject(coder, /*sdkComponents=*/ null));

    Assert.assertThat(reader, new IsInstanceOf(AvroByteReader.class));
    AvroByteReader avroReader = (AvroByteReader) reader;
    Assert.assertEquals(pathToAvroFile, avroReader.avroSource.getFileOrPatternSpec());
    Assert.assertEquals(200L, avroReader.startPosition);
    Assert.assertEquals(500L, avroReader.endPosition);
    Assert.assertEquals(coder, avroReader.coder);
  }
}
