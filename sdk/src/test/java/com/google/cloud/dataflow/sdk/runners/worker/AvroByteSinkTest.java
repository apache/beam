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

import com.google.cloud.dataflow.sdk.TestUtils;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for AvroByteSink.
 */
@RunWith(JUnit4.class)
public class AvroByteSinkTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  <T> void runTestWriteFile(List<T> elems, Coder<T> coder) throws Exception {
    File tmpFile = tmpFolder.newFile("file.avro");
    String filename = tmpFile.getPath();

    // Write the file.

    AvroByteSink<T> avroSink = new AvroByteSink<>(filename, coder);
    List<Long> actualSizes = new ArrayList<>();
    try (Sink.SinkWriter<T> writer = avroSink.writer()) {
      for (T elem : elems) {
        actualSizes.add(writer.add(elem));
      }
    }

    // Read back the file.

    SeekableByteChannel inChannel =
        (SeekableByteChannel) IOChannelUtils.getFactory(filename).open(filename);

    SeekableInput seekableInput = new AvroReader.SeekableByteChannelInput(inChannel);

    Schema schema = Schema.create(Schema.Type.BYTES);

    DatumReader<ByteBuffer> datumReader = new GenericDatumReader<>(schema);

    DataFileReader<ByteBuffer> fileReader = new DataFileReader<>(seekableInput, datumReader);

    List<T> actual = new ArrayList<>();
    List<Long> expectedSizes = new ArrayList<>();
    ByteBuffer inBuffer = ByteBuffer.allocate(10 * 1024);
    while (fileReader.hasNext()) {
      inBuffer = fileReader.next(inBuffer);
      byte[] encodedElem = new byte[inBuffer.remaining()];
      inBuffer.get(encodedElem);
      assert inBuffer.remaining() == 0;
      inBuffer.clear();
      T elem = CoderUtils.decodeFromByteArray(coder, encodedElem);
      actual.add(elem);
      expectedSizes.add((long) encodedElem.length);
    }

    fileReader.close();

    // Compare the expected and the actual elements.
    Assert.assertEquals(elems, actual);
    Assert.assertEquals(expectedSizes, actualSizes);
  }

  @Test
  public void testWriteFile() throws Exception {
    runTestWriteFile(TestUtils.INTS, BigEndianIntegerCoder.of());
  }

  @Test
  public void testWriteEmptyFile() throws Exception {
    runTestWriteFile(TestUtils.NO_INTS, BigEndianIntegerCoder.of());
  }

  // TODO: sharded filenames
  // TODO: writing to GCS
}
