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
package org.apache.beam.sdk.io.tika;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.beam.sdk.io.tika.TikaSource.TikaReader;
import org.junit.Test;

/**
 * Tests TikaReader.
 */
public class TikaReaderTest {
  private static final List<String> ODT_FILE = Arrays.asList(
      "Combining", "can help to ingest", "Apache", "Beam", "in most known formats.",
      "the content from the files", "and", "Apache Tika");

  @Test
  public void testOdtFileAsyncReader() throws Exception {
    doTestOdtFileReader(false);
  }
  @Test
  public void testOdtFileSyncReader() throws Exception {
    doTestOdtFileReader(true);
  }
  private void doTestOdtFileReader(boolean sync) throws Exception {
    String resourcePath = getClass().getResource("/apache-beam-tika1.odt").getPath();
    TikaSource source = new TikaSource(TikaIO.read()
                                                .withParseSynchronously(sync)
                                                .from(resourcePath));
    TikaReader reader = (TikaReader) source.createReader(null);

    List<String> content = new LinkedList<String>();
    for (boolean available = reader.start(); available; available = reader.advance()) {
      content.add(reader.getCurrent());
    }
    assertTrue(content.containsAll(ODT_FILE));
    if (!sync) {
      assertNotNull(reader.getExecutorService());
    } else {
      assertNull(reader.getExecutorService());
    }
    reader.close();
  }

  @Test
  public void testOdtFilesReader() throws Exception {
    String resourcePath = getClass().getResource("/apache-beam-tika1.odt").getPath();
    String filePattern = resourcePath.replace("apache-beam-tika1", "*");

    TikaSource source = new TikaSource(TikaIO.read().from(filePattern));
    TikaSource.FilePatternTikaReader reader =
        (TikaSource.FilePatternTikaReader) source.createReader(null);
    List<String> content = new LinkedList<String>();
    for (boolean available = reader.start(); available; available = reader.advance()) {
      content.add(reader.getCurrent());
    }
    assertTrue(content.containsAll(ODT_FILE));
    reader.close();
  }
}
