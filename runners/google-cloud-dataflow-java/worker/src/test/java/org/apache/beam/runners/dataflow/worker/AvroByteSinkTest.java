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

import static org.apache.beam.runners.dataflow.worker.ReaderUtils.readAllFromReader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.TestUtils;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for AvroByteSink. */
@RunWith(JUnit4.class)
public class AvroByteSinkTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  <T> void runTestWriteFile(List<T> elems, Coder<T> coder) throws Exception {
    File tmpFile = tmpFolder.newFile("file.avro");
    String filename = tmpFile.getPath();

    // Write the file.

    AvroByteSink<T> avroSink =
        new AvroByteSink<>(FileSystems.matchNewResource(filename, false), coder);
    List<Long> actualSizes = new ArrayList<>();
    try (Sink.SinkWriter<T> writer = avroSink.writer()) {
      for (T elem : elems) {
        actualSizes.add(writer.add(elem));
      }
    }

    // Read back the file.
    AvroByteReader<T> reader =
        new AvroByteReader<>(filename, 0L, Long.MAX_VALUE, coder, PipelineOptionsFactory.create());

    List<T> actual = readAllFromReader(reader);
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
