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

package com.google.cloud.dataflow.sdk.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.worker.ReaderUtils;
import com.google.cloud.dataflow.sdk.runners.worker.TextReader;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collection;

/**
 * Tests for IOFactory.
 */
@RunWith(JUnit4.class)
public class IOFactoryTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testLocalFileIO() throws Exception {
    // Create some files to match against.
    File foo1 = tmpFolder.newFile("foo1");
    foo1.createNewFile();
    File foo2 = tmpFolder.newFile("foo2");
    foo2.createNewFile();
    tmpFolder.newFile("barf").createNewFile();

    FileIOChannelFactory factory = new FileIOChannelFactory();
    Collection<String> paths = factory.match(tmpFolder.getRoot().getCanonicalPath() + "/f*");

    Assert.assertEquals(2, paths.size());
    Assert.assertTrue(paths.contains(foo1.getCanonicalPath()));
    Assert.assertTrue(paths.contains(foo2.getCanonicalPath()));
  }

  @Test
  public void testMultiFileRead() throws Exception {
    File file1 = tmpFolder.newFile("file1");
    try (FileOutputStream output = new FileOutputStream(file1)) {
      output.write("1\n2".getBytes());
    }

    File file2 = tmpFolder.newFile("file2");
    try (FileOutputStream output = new FileOutputStream(file2)) {
      output.write("3\n4\n".getBytes());
    }

    File file3 = tmpFolder.newFile("file3");
    try (FileOutputStream output = new FileOutputStream(file3)) {
      output.write("5".getBytes());
    }


    TextReader<String> reader = new TextReader<>(
        tmpFolder.getRoot() + "/file*", true /* strip newlines */, null, null, StringUtf8Coder.of(),
        TextIO.CompressionType.UNCOMPRESSED);

    assertThat(ReaderUtils.readAllFromReader(reader), containsInAnyOrder("1", "2", "3", "4", "5"));
  }
}
