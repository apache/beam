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
package org.apache.beam.sdk.extensions.gcp.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link GcsResourceId}.
 */
@RunWith(JUnit4.class)
public class GcsResourceIdTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testResolve() throws Exception {
    // Tests for common gcs paths.
    assertEquals(
        toResourceIdentifier("gs://bucket/tmp/aa"),
        toResourceIdentifier("gs://bucket/tmp/")
            .resolve("aa", StandardResolveOptions.RESOLVE_FILE));
    assertEquals(
        toResourceIdentifier("gs://bucket/tmp/aa/bb/cc/"),
        toResourceIdentifier("gs://bucket/tmp/")
            .resolve("aa", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("bb", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("cc", StandardResolveOptions.RESOLVE_DIRECTORY));

    // Tests absolute path.
    assertEquals(
        toResourceIdentifier("gs://bucket/tmp/aa"),
        toResourceIdentifier("gs://bucket/tmp/bb/")
            .resolve("gs://bucket/tmp/aa", StandardResolveOptions.RESOLVE_FILE));

    // Tests bucket with no ending '/'.
    assertEquals(
        toResourceIdentifier("gs://my_bucket/tmp"),
        toResourceIdentifier("gs://my_bucket")
            .resolve("tmp", StandardResolveOptions.RESOLVE_FILE));

    // Tests path with unicode
    assertEquals(
        toResourceIdentifier("gs://bucket/输出 目录/输出 文件01.txt"),
        toResourceIdentifier("gs://bucket/输出 目录/")
            .resolve("输出 文件01.txt", StandardResolveOptions.RESOLVE_FILE));
  }

  @Test
  public void testResolveHandleBadInputs() throws Exception {
    assertEquals(
        toResourceIdentifier("gs://my_bucket/tmp/"),
        toResourceIdentifier("gs://my_bucket/")
            .resolve("tmp/", StandardResolveOptions.RESOLVE_DIRECTORY));
  }

  @Test
  public void testResolveInvalidInputs() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("The resolved file: [tmp/] should not end with '/'.");
    toResourceIdentifier("gs://my_bucket/").resolve("tmp/", StandardResolveOptions.RESOLVE_FILE);
  }

  @Test
  public void testResolveInvalidNotDirectory() throws Exception {
    ResourceId tmpDir = toResourceIdentifier("gs://my_bucket/")
        .resolve("tmp dir", StandardResolveOptions.RESOLVE_FILE);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Expected the gcsPath is a directory, but had [gs://my_bucket/tmp dir].");
    tmpDir.resolve("aa", StandardResolveOptions.RESOLVE_FILE);
  }

  @Test
  public void testGetCurrentDirectory() throws Exception {
    // Tests gcs paths.
    assertEquals(
        toResourceIdentifier("gs://my_bucket/tmp dir/"),
        toResourceIdentifier("gs://my_bucket/tmp dir/").getCurrentDirectory());

    // Tests path with unicode.
    assertEquals(
        toResourceIdentifier("gs://my_bucket/输出 目录/"),
        toResourceIdentifier("gs://my_bucket/输出 目录/文件01.txt").getCurrentDirectory());

    // Tests bucket with no ending '/'.
    assertEquals(
        toResourceIdentifier("gs://my_bucket/"),
        toResourceIdentifier("gs://my_bucket").getCurrentDirectory());
  }

  @Test
  public void testIsDirectory() throws Exception {
    assertTrue(toResourceIdentifier("gs://my_bucket/tmp dir/").isDirectory());
    assertTrue(toResourceIdentifier("gs://my_bucket/").isDirectory());
    assertTrue(toResourceIdentifier("gs://my_bucket").isDirectory());

    assertFalse(toResourceIdentifier("gs://my_bucket/file").isDirectory());
  }

  @Test
  public void testInvalidGcsPath() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid GCS URI: gs://");
    toResourceIdentifier("gs://");
  }

  @Test
  public void testGetScheme() throws Exception {
    // Tests gcs paths.
    assertEquals("gs", toResourceIdentifier("gs://my_bucket/tmp dir/").getScheme());

    // Tests bucket with no ending '/'.
    assertEquals("gs", toResourceIdentifier("gs://my_bucket").getScheme());
  }

  @Test
  public void testEquals() throws Exception {
    assertEquals(
        toResourceIdentifier("gs://my_bucket/tmp/"),
        toResourceIdentifier("gs://my_bucket/tmp/"));

    assertNotEquals(
        toResourceIdentifier("gs://my_bucket/tmp"),
        toResourceIdentifier("gs://my_bucket/tmp/"));
  }

  @Test
  public void testGetFilename() throws Exception {
    assertEquals(toResourceIdentifier("gs://my_bucket/").getFilename(), null);
    assertEquals(toResourceIdentifier("gs://my_bucket/abc").getFilename(),
        "abc");
    assertEquals(toResourceIdentifier("gs://my_bucket/abc/").getFilename(),
        "abc");
    assertEquals(toResourceIdentifier("gs://my_bucket/abc/xyz.txt").getFilename(),
        "xyz.txt");
  }

  private GcsResourceId toResourceIdentifier(String str) throws Exception {
    return GcsResourceId.fromGcsPath(GcsPath.fromUri(str));
  }
}
