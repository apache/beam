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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.nio.file.Paths;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdTester;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link LocalResourceId}.
 *
 * <p>TODO: re-enable unicode tests when BEAM-1453 is resolved.
 */
@RunWith(JUnit4.class)
public class LocalResourceIdTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testResolveInUnix() {
    if (SystemUtils.IS_OS_WINDOWS) {
      // Skip tests
      return;
    }
    // Tests for local files without the scheme.
    assertEquals(
        toResourceIdentifier("/root/tmp/aa"),
        toResourceIdentifier("/root/tmp/").resolve("aa", StandardResolveOptions.RESOLVE_FILE));
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
        toResourceIdentifier("file:///").resolve("aa", StandardResolveOptions.RESOLVE_FILE));
  }

  @Test
  public void testResolveNormalizationInUnix() {
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
  public void testResolveHandleBadInputsInUnix() {
    if (SystemUtils.IS_OS_WINDOWS) {
      // Skip tests
      return;
    }
    assertEquals(
        toResourceIdentifier("/root/tmp/"),
        toResourceIdentifier("/root/").resolve("tmp/", StandardResolveOptions.RESOLVE_DIRECTORY));
  }

  @Test
  public void testResolveInvalidInputs() {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10730
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("The resolved file: [tmp/] should not end with '/'.");
    toResourceIdentifier("/root/").resolve("tmp/", StandardResolveOptions.RESOLVE_FILE);
  }

  @Test
  public void testResolveInvalidNotDirectory() {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10731
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    ResourceId tmp =
        toResourceIdentifier("/root/").resolve("tmp", StandardResolveOptions.RESOLVE_FILE);
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Expected the path is a directory, but had [/root/tmp].");
    tmp.resolve("aa", StandardResolveOptions.RESOLVE_FILE);
  }

  @Test
  public void testResolveInWindowsOS() {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10732
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
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
  public void testGetCurrentDirectoryInUnix() {
    // Tests for local files without the scheme.
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10733
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    assertEquals(
        toResourceIdentifier("/root/tmp/"),
        toResourceIdentifier("/root/tmp/").getCurrentDirectory());
    assertEquals(toResourceIdentifier("/"), toResourceIdentifier("/").getCurrentDirectory());

    // Tests path without parent.
    assertEquals(toResourceIdentifier("./"), toResourceIdentifier("output").getCurrentDirectory());
  }

  @Test
  public void testGetScheme() {
    // Tests for local files without the scheme.
    assertEquals("file", toResourceIdentifier("/root/tmp/").getScheme());
  }

  @Test
  public void testEquals() {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10734
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    assertEquals(toResourceIdentifier("/root/tmp/"), toResourceIdentifier("/root/tmp/"));

    assertNotEquals(toResourceIdentifier("/root/tmp"), toResourceIdentifier("/root/tmp/"));
  }

  @Test
  public void testIsDirectory() {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10735
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    assertTrue(toResourceIdentifier("/").isDirectory());
    assertTrue(toResourceIdentifier("/root/tmp/").isDirectory());
    assertFalse(toResourceIdentifier("/root").isDirectory());
  }

  @Test
  public void testToString() throws Exception {
    File someFile = tmpFolder.newFile("somefile");
    LocalResourceId fileResource =
        LocalResourceId.fromPath(someFile.toPath(), /* isDirectory */ false);
    assertThat(fileResource.toString(), not(endsWith(File.separator)));
    assertThat(fileResource.toString(), containsString("somefile"));
    assertThat(fileResource.toString(), startsWith(tmpFolder.getRoot().getAbsolutePath()));

    LocalResourceId dirResource =
        LocalResourceId.fromPath(someFile.toPath(), /* isDirectory */ true);
    assertThat(dirResource.toString(), endsWith(File.separator));
    assertThat(dirResource.toString(), containsString("somefile"));
    assertThat(dirResource.toString(), startsWith(tmpFolder.getRoot().getAbsolutePath()));
  }

  @Test
  public void testGetFilename() {
    assertNull(toResourceIdentifier("/").getFilename());
    assertEquals("tmp", toResourceIdentifier("/root/tmp").getFilename());
    assertEquals("tmp", toResourceIdentifier("/root/tmp/").getFilename());
    assertEquals("xyz.txt", toResourceIdentifier("/root/tmp/xyz.txt").getFilename());
  }

  @Test
  public void testResourceIdTester() {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10736
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    ResourceIdTester.runResourceIdBattery(toResourceIdentifier("/tmp/foo/"));
  }

  private LocalResourceId toResourceIdentifier(String str) {
    boolean isDirectory;
    if (SystemUtils.IS_OS_WINDOWS) {
      isDirectory = str.endsWith("\\");
    } else {
      isDirectory = str.endsWith("/");
    }
    return LocalResourceId.fromPath(Paths.get(str), isDirectory);
  }
}
