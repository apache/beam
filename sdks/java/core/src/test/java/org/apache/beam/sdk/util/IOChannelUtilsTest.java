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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.io.Files;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * Tests for IOChannelUtils.
 */
@RunWith(JUnit4.class)
public class IOChannelUtilsTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testShardFormatExpansion() {
    assertEquals("output-001-of-123.txt",
        IOChannelUtils.constructName("output", "-SSS-of-NNN",
            ".txt",
            1, 123));

    assertEquals("out.txt/part-00042",
        IOChannelUtils.constructName("out.txt", "/part-SSSSS", "",
            42, 100));

    assertEquals("out.txt",
        IOChannelUtils.constructName("ou", "t.t", "xt", 1, 1));

    assertEquals("out0102shard.txt",
        IOChannelUtils.constructName("out", "SSNNshard", ".txt", 1, 2));

    assertEquals("out-2/1.part-1-of-2.txt",
        IOChannelUtils.constructName("out", "-N/S.part-S-of-N",
            ".txt", 1, 2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testShardNameCollision() throws Exception {
    File outFolder = tmpFolder.newFolder();
    String filename = outFolder.toPath().resolve("output").toString();

    IOChannelUtils.create(filename, "", "", 2, "text").close();
    fail("IOChannelUtils.create expected to fail due "
        + "to filename collision");
  }

  @Test
  public void testLargeShardCount() {
    Assert.assertEquals("out-100-of-5000.txt",
        IOChannelUtils.constructName("out", "-SS-of-NN", ".txt",
            100, 5000));
  }

  @Test
  public void testGetSizeBytes() throws Exception {
    String data = "TestData";
    File file = tmpFolder.newFile();
    Files.write(data, file, StandardCharsets.UTF_8);
    assertEquals(data.length(), IOChannelUtils.getSizeBytes(file.getPath()));
  }

  @Test
  public void testResolveSinglePath() throws Exception {
    String expected = tmpFolder.getRoot().toPath().resolve("aa").toString();
    assertEquals(expected, IOChannelUtils.resolve(tmpFolder.getRoot().toString(), "aa"));
  }

  @Test
  public void testResolveMultiplePaths() throws Exception {
    String expected =
        tmpFolder.getRoot().toPath().resolve("aa").resolve("bb").resolve("cc").toString();
    assertEquals(expected,
        IOChannelUtils.resolve(tmpFolder.getRoot().getPath(), "aa", "bb", "cc"));
  }
}
