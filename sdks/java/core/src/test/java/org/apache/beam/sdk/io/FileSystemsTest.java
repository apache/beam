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
package org.apache.beam.sdk.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.net.URI;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileSystems.StandardResolveOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FileSystems}.
 */
@RunWith(JUnit4.class)
public class FileSystemsTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    FileSystems.setDefaultConfigInWorkers(PipelineOptionsFactory.create());
  }

  @Test
  public void testResolve() throws Exception {
    // Tests for local files without the scheme.
    assertEquals(
        "/root/tmp/aa",
        FileSystems.resolve("/root/tmp", "aa"));
    assertEquals(
        "/root/tmp/aa/bb/cc",
        FileSystems.resolve("/root/tmp", "aa", "bb", "cc"));

    // Tests uris with scheme.
    assertEquals(
        "file:/root/tmp/aa",
        FileSystems.resolve("file:/root/tmp", "aa"));
    assertEquals(
        "file:/aa",
        FileSystems.resolve("file:///", "aa"));
    assertEquals(
        "gs://bucket/tmp/aa",
        FileSystems.resolve("gs://bucket/tmp", "aa"));

    // Tests for Windows OS path in URI format.
    assertEquals(
        "file:/C:/home%20dir/a%20b/a%20b",
        FileSystems.resolve("file:/C:/home%20dir", "a%20b", "a%20b"));

    // Tests absolute path.
    assertEquals(
        "/root/tmp/aa",
        FileSystems.resolve("/root/tmp/bb", "/root/tmp/aa"));

    // Tests authority with empty path.
    assertEquals(
        "gs://bucket/staging",
        FileSystems.resolve("gs://bucket/", "staging"));
    assertEquals(
        "gs://bucket/staging",
        FileSystems.resolve("gs://bucket", "staging"));
    assertEquals(
        "gs://bucket/",
        FileSystems.resolve("gs://bucket", "."));

    // Tests empty authority and path.
    assertEquals(
        "file:/aa",
        FileSystems.resolve("file:///", "aa"));

    // Tests normalizing of "." and ".."
    assertEquals(
        "s3://authority/../home/bb",
        FileSystems.resolve("s3://authority/../home/output/..", "aa", "..", "bb"));
    assertEquals(
        "s3://authority/aa/bb",
        FileSystems.resolve("s3://authority/.", "aa", ".", "bb"));
    assertEquals(
        "aa/bb",
        FileSystems.resolve("a/..", "aa", ".", "bb"));
    assertEquals(
        "/aa/bb",
        FileSystems.resolve("/a/..", "aa", ".", "bb"));

    // Tests ".", "./", "..", "../", "~".
    assertEquals(
        "aa/bb",
        FileSystems.resolve(".", "aa", "./", "bb"));
    assertEquals(
        "aa/bb",
        FileSystems.resolve("./", "aa", "./", "bb"));
    assertEquals(
        "../aa/bb",
        FileSystems.resolve("..", "aa", "./", "bb"));
    assertEquals(
        "../aa/bb",
        FileSystems.resolve("../", "aa", "./", "bb"));
    assertEquals(
        "~/aa/bb",
        FileSystems.resolve("~", "aa", "./", "bb"));

    // Tests path with unicode
    assertEquals(
        "/根目录/输出 文件01.txt",
        FileSystems.resolve("/根目录", "输出 文件01.txt"));
    assertEquals(
        "gs://根目录/输出 文件01.txt",
        FileSystems.resolve("gs://根目录", "输出 文件01.txt"));
  }

  @Test
  public void testResolveSibling() throws Exception {
    // Tests for local files without the scheme.
    assertEquals(
        "/root/aa",
        FileSystems.resolve("/root/tmp", "aa", StandardResolveOptions.RESOLVE_SIBLING));
  }


  @Test
  public void testResolveWithPattern() {
    // Test resolve asterisks.
    assertEquals(
        "/root/tmp/**/*",
        FileSystems.resolve("/root/tmp", "**", "*"));

    assertEquals(
        "file:/C:/home/**/*",
        FileSystems.resolve("file:/C:/home", "**", "*"));
  }

  @Test
  public void testResolveInWindowsOS() throws Exception {
    if (SystemUtils.IS_OS_WINDOWS) {
      assertEquals(
          "C:\\my home\\out put",
          FileSystems.resolve("C:\\my home", "out put"));

      assertEquals(
          "C:\\my home\\**\\*",
          FileSystems.resolve("C:\\my home", "**", "*"));
    } else {
      // Skip tests
    }
  }

  @Test
  public void testResolveIllegalURIChars() throws Exception {
    // Tests for Windows OS path in URI format but with illegal chars.
    assertEquals(
        "file:/C:/home dir/a b/a b",
        FileSystems.resolve("file:/C:/home dir", "a b", "a b"));
  }

  @Test
  public void testResolveOtherIsEmptyPath() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected other is not empty.");
    // Tests resolving empty strings.
    FileSystems.resolve("/root/tmp/aa", "", "");
  }

  @Test
  public void testResolveDirectoryHasQuery() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected no query in uri");
    // Tests resolving empty strings.
    FileSystems.resolve("/root/tmp/aa?q", "bb");
  }

  @Test
  public void testResolveDirectoryHasFragment() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected no fragment in uri");
    // Tests resolving empty strings.
    FileSystems.resolve("/root/tmp/aa#q", "bb");
  }

  @Test
  public void testResolveOtherHasQuery() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected no query in uri");
    // Tests resolving empty strings.
    FileSystems.resolve("/root/tmp/aa", "bb?q");
  }

  @Test
  public void testResolveOtherHasFragment() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected no fragment in uri");
    // Tests resolving empty strings.
    FileSystems.resolve("/root/tmp/aa", "bb#q");
  }

  @Test
  public void testGetLowerCaseScheme() throws Exception {
    assertEquals("gs", FileSystems.getLowerCaseScheme("gs://bucket/output"));
    assertEquals("gs", FileSystems.getLowerCaseScheme("GS://bucket/output"));
    assertEquals("file", FileSystems.getLowerCaseScheme("/home/output"));
    assertEquals("file", FileSystems.getLowerCaseScheme("file:///home/output"));
    assertEquals("file", FileSystems.getLowerCaseScheme("C:\\home\\output"));
    assertEquals("file", FileSystems.getLowerCaseScheme("C:\\home\\output"));
  }

  @Test
  public void testGetLocalFileSystem() throws Exception {
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("~/home/")) instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("file://home")) instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("FILE://home")) instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("File://home")) instanceof LocalFileSystem);
  }

  @Test
  public void testVerifySchemesAreUnique() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Scheme: [file] has conflicting registrars");
    FileSystems.verifySchemesAreUnique(
        Sets.<FileSystemRegistrar>newHashSet(
            new LocalFileSystemRegistrar(),
            new FileSystemRegistrar() {
              @Override
              public FileSystem fromOptions(@Nullable PipelineOptions options) {
                return null;
              }

              @Override
              public String getScheme() {
                return "FILE";
              }
            }));
  }
}
