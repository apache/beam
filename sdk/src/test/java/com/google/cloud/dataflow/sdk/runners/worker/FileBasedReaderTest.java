/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.io.TextIO.CompressionType;
import com.google.cloud.dataflow.sdk.runners.worker.FileBasedReader.FilenameBasedStreamFactory;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for FileBasedReader.
 */
@RunWith(JUnit4.class)
public class FileBasedReaderTest {

  private void testGetStreamForAutoHelper(CompressionType expected, String filename) {
    FilenameBasedStreamFactory factory = new FilenameBasedStreamFactory(filename,
        CompressionType.AUTO);
    CompressionType actual = factory.getCompressionTypeForAuto();
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetStreamForAuto() {
    testGetStreamForAutoHelper(CompressionType.UNCOMPRESSED, "test");
    testGetStreamForAutoHelper(CompressionType.UNCOMPRESSED, "test.txt");
    testGetStreamForAutoHelper(CompressionType.GZIP, "test.gz");
    testGetStreamForAutoHelper(CompressionType.BZIP2, "test.bz2");
  }
}
