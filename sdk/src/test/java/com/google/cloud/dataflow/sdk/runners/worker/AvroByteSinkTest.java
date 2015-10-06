/*
 * Copyright (C) 2015 Google Inc.
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
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
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
    AvroByteReader<T> reader = new AvroByteReader<>(filename, null, null, coder, null);


    List<T> actual = new ArrayList<>();
    ReaderTestUtils.readRemainingFromReader(reader, actual);
    List<Long> expectedSizes = new ArrayList<>();

    for (T value : actual) {
      expectedSizes.add((long) CoderUtils.encodeToByteArray(coder, value).length);
    }

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
