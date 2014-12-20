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
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

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
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for AvroSink.
 */
@RunWith(JUnit4.class)
public class AvroSinkTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  <T> void runTestWriteFile(List<T> elems, AvroCoder<T> coder) throws Exception {
    File tmpFile = tmpFolder.newFile("file.avro");
    String filename = tmpFile.getPath();

    // Write the file.

    AvroSink<T> avroSink = new AvroSink<>(filename, WindowedValue.getValueOnlyCoder(coder));
    List<Long> actualSizes = new ArrayList<>();
    try (Sink.SinkWriter<WindowedValue<T>> writer = avroSink.writer()) {
      for (T elem : elems) {
        actualSizes.add(writer.add(WindowedValue.valueInGlobalWindow(elem)));
      }
    }

    // Read back the file.

    SeekableByteChannel inChannel =
        (SeekableByteChannel) IOChannelUtils.getFactory(filename).open(filename);

    SeekableInput seekableInput = new AvroReader.SeekableByteChannelInput(inChannel);

    DatumReader<T> datumReader = new GenericDatumReader<>(coder.getSchema());

    DataFileReader<T> fileReader = new DataFileReader<>(seekableInput, datumReader);

    List<T> actual = new ArrayList<>();
    List<Long> expectedSizes = new ArrayList<>();
    while (fileReader.hasNext()) {
      T next = fileReader.next();
      actual.add(next);
      expectedSizes.add((long) CoderUtils.encodeToByteArray(coder, next).length);
    }

    fileReader.close();

    // Compare the expected and the actual elements.
    Assert.assertEquals(elems, actual);
    Assert.assertEquals(expectedSizes, actualSizes);
  }

  @Test
  public void testWriteFile() throws Exception {
    runTestWriteFile(TestUtils.INTS, AvroCoder.of(Integer.class));
  }

  @Test
  public void testWriteEmptyFile() throws Exception {
    runTestWriteFile(TestUtils.NO_INTS, AvroCoder.of(Integer.class));
  }

  // TODO: sharded filenames
  // TODO: writing to GCS
}
