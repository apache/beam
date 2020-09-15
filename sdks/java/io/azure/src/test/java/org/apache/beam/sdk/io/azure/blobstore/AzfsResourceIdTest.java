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
package org.apache.beam.sdk.io.azure.blobstore;

import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.azure.options.BlobstoreOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdTester;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class AzfsResourceIdTest {
  @RunWith(Parameterized.class)
  public static class ResolveTest {
    @Parameterized.Parameter(0)
    public String baseUri;

    @Parameterized.Parameter(1)
    public String relativePath;

    @Parameterized.Parameter(2)
    public ResolveOptions.StandardResolveOptions resolveOptions;

    @Parameterized.Parameter(3)
    public String expectedResult;

    @Parameterized.Parameters
    public static Collection<Object[]> paths() {
      return Arrays.asList(
          new Object[][] {
            {"azfs://account/container/", "", RESOLVE_DIRECTORY, "azfs://account/container/"},
            {"azfs://account/container", "", RESOLVE_DIRECTORY, "azfs://account/container/"},
            {
              "azfs://account/container",
              "path/to/dir",
              RESOLVE_DIRECTORY,
              "azfs://account/container/path/to/dir/"
            },
            {
              "azfs://account/container",
              "path/to/object",
              RESOLVE_FILE,
              "azfs://account/container/path/to/object"
            },
            {
              "azfs://account/container/path/to/dir/",
              "..",
              RESOLVE_DIRECTORY,
              "azfs://account/container/path/to/"
            },
            // Tests for common Azure paths.
            {
              "azfs://account/container/tmp/", "aa", RESOLVE_FILE, "azfs://account/container/tmp/aa"
            },
            // Tests absolute path.
            {
              "azfs://account/container/tmp/bb/",
              "azfs://account/container/tmp/aa",
              RESOLVE_FILE,
              "azfs://account/container/tmp/aa"
            },
            // Tests container with no ending '/'.
            {"azfs://account/my-container", "tmp", RESOLVE_FILE, "azfs://account/my-container/tmp"},
            // Tests path with unicode
            {
              "azfs://account/container/输出 目录/",
              "输出 文件01.txt",
              RESOLVE_FILE,
              "azfs://account/container/输出 目录/输出 文件01.txt"
            }
          });
    }

    @Test
    public void testResolve() {
      ResourceId resourceId = AzfsResourceId.fromUri(baseUri);
      ResourceId resolved = resourceId.resolve(relativePath, resolveOptions);
      assertEquals(expectedResult, resolved.toString());
    }
  }

  @RunWith(JUnit4.class)
  public static class NonParameterizedTests {
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testMultipleResolves() {
      assertEquals(
          AzfsResourceId.fromUri("azfs://account/container/tmp/aa/bb/cc/"),
          AzfsResourceId.fromUri("azfs://account/container/tmp/")
              .resolve("aa", RESOLVE_DIRECTORY)
              .resolve("bb", RESOLVE_DIRECTORY)
              .resolve("cc", RESOLVE_DIRECTORY));
    }

    @Test
    public void testResolveInvalidInputs() {
      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Cannot resolve a file with a directory path: [tmp/]");
      AzfsResourceId.fromUri("azfs://account/my_container/").resolve("tmp/", RESOLVE_FILE);
    }

    @Test
    public void testResolveInvalidNotDirectory() {
      ResourceId tmpDir =
          AzfsResourceId.fromUri("azfs://account/my_container/").resolve("tmp dir", RESOLVE_FILE);
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage(
          "Expected this resource to be a directory, but was [azfs://account/my_container/tmp dir]");
      tmpDir.resolve("aa", RESOLVE_FILE);
    }

    @Test
    public void testResolveWithFileBase() {
      ResourceId resourceId = AzfsResourceId.fromUri("azfs://account/container/path/to/file");
      thrown.expect(IllegalStateException.class);
      resourceId.resolve("child-path", RESOLVE_DIRECTORY); // resource is not a directory
    }

    @Test
    public void testResolveParentToFile() {
      ResourceId resourceId = AzfsResourceId.fromUri("azfs://account/container/path/to/dir/");
      thrown.expect(IllegalArgumentException.class);
      resourceId.resolve("..", RESOLVE_FILE); // '..' only resolves as dir, not as file
    }

    @Test
    public void testEquals() {
      AzfsResourceId a = AzfsResourceId.fromComponents("account", "container", "a/b/c");
      AzfsResourceId b = AzfsResourceId.fromComponents("account", "container", "a/b/c");
      assertEquals(a, b);
      b = AzfsResourceId.fromComponents(a.getAccount(), a.getContainer(), "a/b/c/");
      assertNotEquals(a, b);
      b = AzfsResourceId.fromComponents(a.getAccount(), a.getContainer(), "x/y/z");
      assertNotEquals(a, b);
      b = AzfsResourceId.fromComponents(a.getAccount(), "other-container", a.getBlob());
      assertNotEquals(a, b);
      b = AzfsResourceId.fromComponents("other-account", a.getContainer(), a.getBlob());
      assertNotEquals(a, b);
      assertEquals(
          AzfsResourceId.fromUri("azfs://account/container"),
          AzfsResourceId.fromUri("azfs://account/container/"));
    }

    @Test
    public void testFromComponents() {
      AzfsResourceId resourceId = AzfsResourceId.fromComponents("account", "container", "blob");
      assertEquals("azfs", resourceId.getScheme());
      assertEquals("account", resourceId.getAccount());
      assertEquals("container", resourceId.getContainer());
      assertEquals("blob", resourceId.getBlob());
      assertEquals(
          "virtualDir/blob",
          AzfsResourceId.fromComponents("account", "container", "virtualDir/blob").getBlob());
      assertEquals(null, AzfsResourceId.fromComponents("account", "container").getBlob());
      assertEquals(null, AzfsResourceId.fromComponents("account", "container", "").getBlob());
      assertEquals(null, AzfsResourceId.fromComponents("account", "container", null).getBlob());
    }

    @Test
    public void testFromUri() {
      AzfsResourceId resourceId = AzfsResourceId.fromUri("azfs://account/container/blob");
      assertEquals("azfs", resourceId.getScheme());
      assertEquals("account", resourceId.getAccount());
      assertEquals("container", resourceId.getContainer());
      assertEquals("blob", resourceId.getBlob());
      assertEquals(
          "virtualDir/blob",
          AzfsResourceId.fromUri("azfs://account/container/virtualDir/blob").getBlob());
      assertEquals(null, AzfsResourceId.fromUri("azfs://account/container").getBlob());
    }

    @Test
    public void testIsDirectory() {
      assertTrue(AzfsResourceId.fromUri("azfs://account/container/virtualDir/").isDirectory());
      assertTrue(AzfsResourceId.fromUri("azfs://account/container").isDirectory());
      assertFalse(AzfsResourceId.fromUri("azfs://account/container/virtualDir/blob").isDirectory());
    }

    @Test
    public void testGetCurrentDirectory() {
      // test azfs path
      assertEquals(
          AzfsResourceId.fromUri("azfs://account/container/virtualDir/"),
          AzfsResourceId.fromUri("azfs://account/container/virtualDir/").getCurrentDirectory());
      // test path with unicode
      assertEquals(
          AzfsResourceId.fromUri("azfs://account/container/输出 目录/"),
          AzfsResourceId.fromUri("azfs://account/container/输出 目录/文件01.txt").getCurrentDirectory());
      // test path without ending '/'
      assertEquals(
          AzfsResourceId.fromUri("azfs://account/container"),
          AzfsResourceId.fromUri("azfs://account/container").getCurrentDirectory());
      assertEquals(
          AzfsResourceId.fromUri("azfs://account/container/"),
          AzfsResourceId.fromUri("azfs://account/container/blob").getCurrentDirectory());
    }

    @Test
    public void testInvalidPathNoContainer() {
      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Invalid AZFS URI: [azfs://]");
      AzfsResourceId.fromUri("azfs://");
    }

    @Test
    public void testInvalidPathNoContainerAndSlash() {
      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Invalid AZFS URI: [azfs:///]");
      AzfsResourceId.fromUri("azfs:///");
    }

    @Test
    public void testGetScheme() {
      // Tests azfs paths.
      assertEquals(
          "azfs", AzfsResourceId.fromUri("azfs://account/container/virtualDir/").getScheme());
      // Tests bucket with no ending '/'.
      assertEquals("azfs", AzfsResourceId.fromUri("azfs://account/container").getScheme());
    }

    @Test
    public void testGetFilename() {
      assertNull(AzfsResourceId.fromUri("azfs://account/container").getFilename());
      assertEquals("blob", AzfsResourceId.fromUri("azfs://account/container/blob").getFilename());
      assertEquals("blob", AzfsResourceId.fromUri("azfs://account/container/blob/").getFilename());
      assertEquals(
          "blob", AzfsResourceId.fromUri("azfs://account/container/virtualDir/blob").getFilename());
      assertEquals(
          "blob",
          AzfsResourceId.fromUri("azfs://account/container/virtualDir/blob/").getFilename());
      assertEquals(
          "blob.txt",
          AzfsResourceId.fromUri("azfs://account/container/virtualDir/blob.txt/").getFilename());
    }

    @Test
    public void testContainerParsing() {
      AzfsResourceId path1 = AzfsResourceId.fromUri("azfs://account/container");
      AzfsResourceId path2 = AzfsResourceId.fromUri("azfs://account/container/");
      assertEquals(path1, path2);
      assertEquals(path1.toString(), path2.toString());
    }

    @Test
    public void testAzfsResourceIdToString() {
      String filename = "azfs://account/container/dir/file.txt";
      AzfsResourceId path = AzfsResourceId.fromUri(filename);
      assertEquals(filename, path.toString());
      filename = "azfs://account/container/blob/";
      path = AzfsResourceId.fromUri(filename);
      assertEquals(filename, path.toString());
      filename = "azfs://account/container/";
      path = AzfsResourceId.fromUri(filename);
      assertEquals(filename, path.toString());
    }

    @Test
    public void testInvalidAzfsResourceId() {
      thrown.expect(IllegalArgumentException.class);
      AzfsResourceId.fromUri("file://an/invalid/azfs/path");
    }

    @Test
    public void testInvalidContainer() {
      thrown.expect(IllegalArgumentException.class);
      AzfsResourceId.fromComponents("account", "invalid/", "");
    }

    @Test
    public void testIsWildcard() {
      assertTrue(AzfsResourceId.fromUri("azfs://account/container/dir/*.txt").isWildcard());
      assertTrue(AzfsResourceId.fromUri("azfs://account/container/a?c/glob").isWildcard());
      assertTrue(AzfsResourceId.fromUri("azfs://account/container/a[bcd]e/glob").isWildcard());
      assertFalse(AzfsResourceId.fromComponents("account", "container").isWildcard());
    }

    @Test
    public void testResourceIdTester() {
      BlobstoreOptions options = PipelineOptionsFactory.create().as(BlobstoreOptions.class);
      FileSystems.setDefaultPipelineOptions(options);
      ResourceIdTester.runResourceIdBattery(
          AzfsResourceId.fromUri("azfs://account/container/blob/"));
    }
  }
}
