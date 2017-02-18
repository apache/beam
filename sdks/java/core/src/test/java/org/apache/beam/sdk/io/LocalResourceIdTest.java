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
import static org.junit.Assert.assertNotEquals;

import java.nio.file.Paths;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link LocalResourceId}.
 *
 * <p>TODO: re-enable unicode tests when BEAM-1453 is resolved.
 */
@RunWith(JUnit4.class)
public class LocalResourceIdTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testResolveInUnix() throws Exception {
    if (SystemUtils.IS_OS_WINDOWS) {
      // Skip tests
      return;
    }
    // Tests for local files without the scheme.
    assertEquals(
        toResourceIdentifier("/root/tmp/aa"),
        toResourceIdentifier("/root/tmp/")
            .resolve("aa", StandardResolveOptions.RESOLVE_FILE));
    assertEquals(
        toResourceIdentifier("/root/tmp/aa/bb/cc/"),
        toResourceIdentifier("/root/tmp/")
            .resolve("aa", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("bb", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("cc", StandardResolveOptions.RESOLVE_DIRECTORY));

    // Tests absolute path.
    assertEquals(
        toResourceIdentifier("/root/tmp/aa"),
        toResourceIdentifier("/root/tmp/bb/")
            .resolve("/root/tmp/aa", StandardResolveOptions.RESOLVE_FILE));

    // Tests empty authority and path.
    assertEquals(
        toResourceIdentifier("file:/aa"),
        toResourceIdentifier("file:///")
            .resolve("aa", StandardResolveOptions.RESOLVE_FILE));
  }

  @Test
  public void testResolveNormalizationInUnix() throws Exception {
    if (SystemUtils.IS_OS_WINDOWS) {
      // Skip tests
      return;
    }
    // Tests normalization of "." and ".."
    //
    // Normalization is the implementation choice of LocalResourceId,
    // and it is not required by ResourceId.resolve().
    assertEquals(
        toResourceIdentifier("file://home/bb"),
        toResourceIdentifier("file://root/../home/output/../")
            .resolve("aa", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("..", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("bb", StandardResolveOptions.RESOLVE_FILE));
    assertEquals(
        toResourceIdentifier("file://root/aa/bb"),
        toResourceIdentifier("file://root/./")
            .resolve("aa", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve(".", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("bb", StandardResolveOptions.RESOLVE_FILE));
    assertEquals(
        toResourceIdentifier("aa/bb"),
        toResourceIdentifier("a/../")
            .resolve("aa", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve(".", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("bb", StandardResolveOptions.RESOLVE_FILE));
    assertEquals(
        toResourceIdentifier("/aa/bb"),
        toResourceIdentifier("/a/../")
            .resolve("aa", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve(".", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("bb", StandardResolveOptions.RESOLVE_FILE));

    // Tests "./", "../", "~/".
    assertEquals(
        toResourceIdentifier("aa/bb"),
        toResourceIdentifier("./")
            .resolve("aa", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve(".", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("bb", StandardResolveOptions.RESOLVE_FILE));
    assertEquals(
        toResourceIdentifier("../aa/bb"),
        toResourceIdentifier("../")
            .resolve("aa", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve(".", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("bb", StandardResolveOptions.RESOLVE_FILE));
    assertEquals(
        toResourceIdentifier("~/aa/bb/"),
        toResourceIdentifier("~/")
            .resolve("aa", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve(".", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("bb", StandardResolveOptions.RESOLVE_DIRECTORY));
  }

  @Test
  public void testResolveHandleBadInputsInUnix() throws Exception {
    if (SystemUtils.IS_OS_WINDOWS) {
      // Skip tests
      return;
    }
    assertEquals(
        toResourceIdentifier("/root/tmp/"),
        toResourceIdentifier("/root/")
            .resolve("tmp/", StandardResolveOptions.RESOLVE_DIRECTORY));
  }

  @Test
  public void testResolveInvalidInputs() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("The resolved file: [tmp/] should not end with '/'.");
    toResourceIdentifier("/root/").resolve("tmp/", StandardResolveOptions.RESOLVE_FILE);
  }

  @Test
  public void testResolveInvalidNotDirectory() throws Exception {
    ResourceId tmp = toResourceIdentifier("/root/")
        .resolve("tmp", StandardResolveOptions.RESOLVE_FILE);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Expected the path is a directory, but had [/root/tmp].");
    tmp.resolve("aa", StandardResolveOptions.RESOLVE_FILE);
  }

  @Test
  public void testResolveInWindowsOS() throws Exception {
    if (!SystemUtils.IS_OS_WINDOWS) {
      // Skip tests
      return;
    }
    assertEquals(
        toResourceIdentifier("C:\\my home\\out put"),
        toResourceIdentifier("C:\\my home\\")
            .resolve("out put", StandardResolveOptions.RESOLVE_FILE));

    assertEquals(
        toResourceIdentifier("C:\\out put"),
        toResourceIdentifier("C:\\my home\\")
            .resolve("..", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve(".", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("out put", StandardResolveOptions.RESOLVE_FILE));

    assertEquals(
        toResourceIdentifier("C:\\my home\\**\\*"),
        toResourceIdentifier("C:\\my home\\")
            .resolve("**", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("*", StandardResolveOptions.RESOLVE_FILE));
  }

  @Test
  public void testGetCurrentDirectoryInUnix() throws Exception {
    // Tests for local files without the scheme.
    assertEquals(
        toResourceIdentifier("/root/tmp/"),
        toResourceIdentifier("/root/tmp/").getCurrentDirectory());
    assertEquals(
        toResourceIdentifier("/"),
        toResourceIdentifier("/").getCurrentDirectory());

    // Tests path without parent.
    assertEquals(
        toResourceIdentifier("./"),
        toResourceIdentifier("output").getCurrentDirectory());
  }

  @Test
  public void testGetScheme() throws Exception {
    // Tests for local files without the scheme.
    assertEquals("file", toResourceIdentifier("/root/tmp/").getScheme());
  }

  @Test
  public void testEquals() throws Exception {
    assertEquals(
        toResourceIdentifier("/root/tmp/"),
        toResourceIdentifier("/root/tmp/"));

    assertNotEquals(
        toResourceIdentifier("/root/tmp"),
        toResourceIdentifier("/root/tmp/"));
  }

  private LocalResourceId toResourceIdentifier(String str) throws Exception {
    boolean isDirectory;
    if (SystemUtils.IS_OS_WINDOWS) {
      isDirectory = str.endsWith("\\");
    } else {
      isDirectory = str.endsWith("/");
    }
    return LocalResourceId.fromPath(Paths.get(str), isDirectory);
  }
}
