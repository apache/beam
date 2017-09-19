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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.tika.TikaSource.TikaReader;
import org.junit.Test;

/**
 * Tests TikaSource.
 */
public class TikaSourceTest {

  @Test
  public void testOdtFileSource() throws Exception {
    String resourcePath = getClass().getResource("/apache-beam-tika1.odt").getPath();
    TikaSource source = new TikaSource(TikaIO.read().from(resourcePath));
    assertEquals(StringUtf8Coder.of(), source.getDefaultOutputCoder());

    assertEquals(TikaSource.Mode.FILEPATTERN, source.getMode());
    assertTrue(source.createReader(null) instanceof TikaReader);

    List<? extends TikaSource> sources = source.split(1, null);
    assertEquals(1, sources.size());
    TikaSource nextSource = sources.get(0);
    assertEquals(TikaSource.Mode.SINGLE_FILE, nextSource.getMode());
    assertEquals(resourcePath, nextSource.getSingleFileMetadata().resourceId().toString());
  }

  @Test
  public void testOdtFilesSource() throws Exception {
    String resourcePath = getClass().getResource("/apache-beam-tika1.odt").getPath();
    String resourcePath2 = getClass().getResource("/apache-beam-tika2.odt").getPath();
    String filePattern = resourcePath.replace("apache-beam-tika1", "*");

    TikaSource source = new TikaSource(TikaIO.read().from(filePattern));
    assertEquals(StringUtf8Coder.of(), source.getDefaultOutputCoder());

    assertEquals(TikaSource.Mode.FILEPATTERN, source.getMode());
    assertTrue(source.createReader(null) instanceof TikaSource.FilePatternTikaReader);

    List<? extends TikaSource> sources = source.split(1, null);
    assertEquals(2, sources.size());
    TikaSource nextSource = sources.get(0);
    assertEquals(TikaSource.Mode.SINGLE_FILE, nextSource.getMode());
    String nextSourceResource = nextSource.getSingleFileMetadata().resourceId().toString();
    TikaSource nextSource2 = sources.get(1);
    assertEquals(TikaSource.Mode.SINGLE_FILE, nextSource2.getMode());
    String nextSourceResource2 = nextSource2.getSingleFileMetadata().resourceId().toString();
    assertTrue(nextSourceResource.equals(resourcePath) && nextSourceResource2.equals(resourcePath2)
        || nextSourceResource.equals(resourcePath2) && nextSourceResource2.equals(resourcePath));
  }
}
