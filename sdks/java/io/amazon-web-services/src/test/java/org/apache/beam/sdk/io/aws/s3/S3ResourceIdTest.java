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
package org.apache.beam.sdk.io.aws.s3;

import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdTester;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link S3ResourceId}. */
@RunWith(JUnit4.class)
public class S3ResourceIdTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  static final class TestCase {

    final String baseUri;
    final String relativePath;
    final StandardResolveOptions resolveOptions;
    final String expectedResult;

    TestCase(
        String baseUri,
        String relativePath,
        StandardResolveOptions resolveOptions,
        String expectedResult) {
      this.baseUri = baseUri;
      this.relativePath = relativePath;
      this.resolveOptions = resolveOptions;
      this.expectedResult = expectedResult;
    }
  }

  // Each test case is an expected URL, then the components used to build it.
  // Empty components result in a double slash.
  private static final List<TestCase> PATH_TEST_CASES =
      Arrays.asList(
          new TestCase("s3://bucket/", "", RESOLVE_DIRECTORY, "s3://bucket/"),
          new TestCase("s3://bucket", "", RESOLVE_DIRECTORY, "s3://bucket/"),
          new TestCase("s3://bucket", "path/to/dir", RESOLVE_DIRECTORY, "s3://bucket/path/to/dir/"),
          new TestCase("s3://bucket", "path/to/object", RESOLVE_FILE, "s3://bucket/path/to/object"),
          new TestCase(
              "s3://bucket/path/to/dir/", "..", RESOLVE_DIRECTORY, "s3://bucket/path/to/"));

  private S3ResourceId deserializeFromB64(String base64) throws Exception {
    ByteArrayInputStream b = new ByteArrayInputStream(Base64.getDecoder().decode(base64));
    try (ObjectInputStream s = new ObjectInputStream(b)) {
      return (S3ResourceId) s.readObject();
    }
  }

  private String serializeToB64(S3ResourceId r) throws Exception {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    try (ObjectOutputStream s = new ObjectOutputStream(b)) {
      s.writeObject(r);
    }
    return Base64.getEncoder().encodeToString(b.toByteArray());
  }

  @Test
  public void testSerialization() throws Exception {
    String r1Serialized =
        "rO0ABXNyACpvcmcuYXBhY2hlLmJlYW0uc2RrLmlvLmF3cy5zMy5TM1Jlc291cmNlSWSN8nM8V4cVFwIABEwABmJ1Y2tldHQAEkxqYXZhL2xhbmcvU3RyaW5nO0wAA2tleXEAfgABTAAMbGFzdE1vZGlmaWVkdAAQTGphdmEvdXRpbC9EYXRlO0wABHNpemV0ABBMamF2YS9sYW5nL0xvbmc7eHB0AAZidWNrZXR0AAYvYS9iL2NwcA==";
    String r2Serialized =
        "rO0ABXNyACpvcmcuYXBhY2hlLmJlYW0uc2RrLmlvLmF3cy5zMy5TM1Jlc291cmNlSWSN8nM8V4cVFwIABEwABmJ1Y2tldHQAEkxqYXZhL2xhbmcvU3RyaW5nO0wAA2tleXEAfgABTAAMbGFzdE1vZGlmaWVkdAAQTGphdmEvdXRpbC9EYXRlO0wABHNpemV0ABBMamF2YS9sYW5nL0xvbmc7eHB0AAxvdGhlci1idWNrZXR0AAYveC95L3pwc3IADmphdmEubGFuZy5Mb25nO4vkkMyPI98CAAFKAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAAAAAB7";
    String r3Serialized =
        "rO0ABXNyACpvcmcuYXBhY2hlLmJlYW0uc2RrLmlvLmF3cy5zMy5TM1Jlc291cmNlSWSN8nM8V4cVFwIABEwABmJ1Y2tldHQAEkxqYXZhL2xhbmcvU3RyaW5nO0wAA2tleXEAfgABTAAMbGFzdE1vZGlmaWVkdAAQTGphdmEvdXRpbC9EYXRlO0wABHNpemV0ABBMamF2YS9sYW5nL0xvbmc7eHB0AAx0aGlyZC1idWNrZXR0AAkvZm9vL2Jhci9zcgAOamF2YS51dGlsLkRhdGVoaoEBS1l0GQMAAHhwdwgAADgCgmXOAHhw";
    String r4Serialized =
        "rO0ABXNyACpvcmcuYXBhY2hlLmJlYW0uc2RrLmlvLmF3cy5zMy5TM1Jlc291cmNlSWSN8nM8V4cVFwIABEwABmJ1Y2tldHQAEkxqYXZhL2xhbmcvU3RyaW5nO0wAA2tleXEAfgABTAAMbGFzdE1vZGlmaWVkdAAQTGphdmEvdXRpbC9EYXRlO0wABHNpemV0ABBMamF2YS9sYW5nL0xvbmc7eHB0AApiYXotYnVja2V0dAAGL2EvYi9jc3IADmphdmEudXRpbC5EYXRlaGqBAUtZdBkDAAB4cHcIAAA33gSV5gB4c3IADmphdmEubGFuZy5Mb25nO4vkkMyPI98CAAFKAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAAAAAAq";

    S3ResourceId r1 = S3ResourceId.fromComponents("s3", "bucket", "a/b/c");
    S3ResourceId r2 = S3ResourceId.fromComponents("s3", "other-bucket", "x/y/z").withSize(123);
    S3ResourceId r3 =
        S3ResourceId.fromComponents("s3", "third-bucket", "foo/bar/")
            .withLastModified(new Date(121, Calendar.JULY, 3));
    S3ResourceId r4 =
        S3ResourceId.fromComponents("s3", "baz-bucket", "a/b/c")
            .withSize(42)
            .withLastModified(new Date(116, Calendar.JULY, 15));
    S3ResourceId r5 = S3ResourceId.fromComponents("other-scheme", "bucket", "a/b/c");
    S3ResourceId r6 =
        S3ResourceId.fromComponents("other-scheme", "baz-bucket", "foo/bar/")
            .withSize(42)
            .withLastModified(new Date(116, Calendar.JULY, 5));

    // S3ResourceIds serialized by previous versions should still deserialize.
    assertEquals(r1, deserializeFromB64(r1Serialized));
    assertEquals(r2, deserializeFromB64(r2Serialized));
    assertEquals(r3, deserializeFromB64(r3Serialized));
    assertEquals(r4, deserializeFromB64(r4Serialized));

    // Current resource IDs should round-trip properly through serialization.
    assertEquals(r1, deserializeFromB64(serializeToB64(r1)));
    assertEquals(r2, deserializeFromB64(serializeToB64(r2)));
    assertEquals(r3, deserializeFromB64(serializeToB64(r3)));
    assertEquals(r4, deserializeFromB64(serializeToB64(r4)));
    assertEquals(r5, deserializeFromB64(serializeToB64(r5)));
    assertEquals(r6, deserializeFromB64(serializeToB64(r6)));
  }

  @Test
  public void testResolve() {
    for (TestCase testCase : PATH_TEST_CASES) {
      ResourceId resourceId = S3ResourceId.fromUri(testCase.baseUri);
      ResourceId resolved = resourceId.resolve(testCase.relativePath, testCase.resolveOptions);
      assertEquals(testCase.expectedResult, resolved.toString());
    }

    // Tests for common s3 paths.
    assertEquals(
        S3ResourceId.fromUri("s3://bucket/tmp/aa"),
        S3ResourceId.fromUri("s3://bucket/tmp/").resolve("aa", RESOLVE_FILE));
    assertEquals(
        S3ResourceId.fromUri("s3://bucket/tmp/aa/bb/cc/"),
        S3ResourceId.fromUri("s3://bucket/tmp/")
            .resolve("aa", RESOLVE_DIRECTORY)
            .resolve("bb", RESOLVE_DIRECTORY)
            .resolve("cc", RESOLVE_DIRECTORY));

    // Tests absolute path.
    assertEquals(
        S3ResourceId.fromUri("s3://bucket/tmp/aa"),
        S3ResourceId.fromUri("s3://bucket/tmp/bb/").resolve("s3://bucket/tmp/aa", RESOLVE_FILE));

    // Tests bucket with no ending '/'.
    assertEquals(
        S3ResourceId.fromUri("s3://my-bucket/tmp"),
        S3ResourceId.fromUri("s3://my-bucket").resolve("tmp", RESOLVE_FILE));

    // Tests path with unicode
    assertEquals(
        S3ResourceId.fromUri("s3://bucket/输出 目录/输出 文件01.txt"),
        S3ResourceId.fromUri("s3://bucket/输出 目录/").resolve("输出 文件01.txt", RESOLVE_FILE));
  }

  @Test
  public void testResolveInvalidInputs() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Cannot resolve a file with a directory path: [tmp/]");
    S3ResourceId.fromUri("s3://my_bucket/").resolve("tmp/", RESOLVE_FILE);
  }

  @Test
  public void testResolveInvalidNotDirectory() {
    ResourceId tmpDir = S3ResourceId.fromUri("s3://my_bucket/").resolve("tmp dir", RESOLVE_FILE);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Expected this resource to be a directory, but was [s3://my_bucket/tmp dir]");
    tmpDir.resolve("aa", RESOLVE_FILE);
  }

  @Test
  public void testS3ResolveWithFileBase() {
    ResourceId resourceId = S3ResourceId.fromUri("s3://bucket/path/to/file");
    thrown.expect(IllegalStateException.class);
    resourceId.resolve("child-path", RESOLVE_DIRECTORY); // resource is not a directory
  }

  @Test
  public void testResolveParentToFile() {
    ResourceId resourceId = S3ResourceId.fromUri("s3://bucket/path/to/dir/");
    thrown.expect(IllegalArgumentException.class);
    resourceId.resolve("..", RESOLVE_FILE); // '..' only resolves as dir, not as file
  }

  @Test
  public void testGetCurrentDirectory() {
    // Tests s3 paths.
    assertEquals(
        S3ResourceId.fromUri("s3://my_bucket/tmp dir/"),
        S3ResourceId.fromUri("s3://my_bucket/tmp dir/").getCurrentDirectory());

    // Tests path with unicode.
    assertEquals(
        S3ResourceId.fromUri("s3://my_bucket/输出 目录/"),
        S3ResourceId.fromUri("s3://my_bucket/输出 目录/文件01.txt").getCurrentDirectory());

    // Tests bucket with no ending '/'.
    assertEquals(
        S3ResourceId.fromUri("s3://my_bucket/"),
        S3ResourceId.fromUri("s3://my_bucket").getCurrentDirectory());
    assertEquals(
        S3ResourceId.fromUri("s3://my_bucket/"),
        S3ResourceId.fromUri("s3://my_bucket/not-directory").getCurrentDirectory());
  }

  @Test
  public void testIsDirectory() {
    assertTrue(S3ResourceId.fromUri("s3://my_bucket/tmp dir/").isDirectory());
    assertTrue(S3ResourceId.fromUri("s3://my_bucket/").isDirectory());
    assertTrue(S3ResourceId.fromUri("s3://my_bucket").isDirectory());
    assertFalse(S3ResourceId.fromUri("s3://my_bucket/file").isDirectory());
  }

  @Test
  public void testInvalidPathNoBucket() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid S3 URI: [s3://]");
    S3ResourceId.fromUri("s3://");
  }

  @Test
  public void testInvalidPathNoBucketAndSlash() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid S3 URI: [s3:///]");
    S3ResourceId.fromUri("s3:///");
  }

  @Test
  public void testGetScheme() {
    // Tests s3 paths.
    assertEquals("s3", S3ResourceId.fromUri("s3://my_bucket/tmp dir/").getScheme());

    // Tests bucket with no ending '/'.
    assertEquals("s3", S3ResourceId.fromUri("s3://my_bucket").getScheme());
  }

  @Test
  public void testGetFilename() {
    assertNull(S3ResourceId.fromUri("s3://my_bucket/").getFilename());
    assertEquals("abc", S3ResourceId.fromUri("s3://my_bucket/abc").getFilename());
    assertEquals("abc", S3ResourceId.fromUri("s3://my_bucket/abc/").getFilename());
    assertEquals("def", S3ResourceId.fromUri("s3://my_bucket/abc/def").getFilename());
    assertEquals("def", S3ResourceId.fromUri("s3://my_bucket/abc/def/").getFilename());
    assertEquals("xyz.txt", S3ResourceId.fromUri("s3://my_bucket/abc/xyz.txt").getFilename());
  }

  @Test
  public void testParentRelationship() {
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/subdir/object");
    assertEquals("bucket", path.getBucket());
    assertEquals("dir/subdir/object", path.getKey());

    // s3://bucket/dir/
    path = S3ResourceId.fromUri("s3://bucket/dir/subdir/");
    S3ResourceId parent = (S3ResourceId) path.resolve("..", RESOLVE_DIRECTORY);
    assertEquals("bucket", parent.getBucket());
    assertEquals("dir/", parent.getKey());
    assertNotEquals(path, parent);
    assertTrue(path.getKey().startsWith(parent.getKey()));
    assertFalse(parent.getKey().startsWith(path.getKey()));

    // s3://bucket/
    S3ResourceId grandParent = (S3ResourceId) parent.resolve("..", RESOLVE_DIRECTORY);
    assertEquals("bucket", grandParent.getBucket());
    assertEquals("", grandParent.getKey());
  }

  @Test
  public void testBucketParsing() {
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket");
    S3ResourceId path2 = S3ResourceId.fromUri("s3://bucket/");

    assertEquals(path, path2);
    assertEquals(path.toString(), path2.toString());
  }

  @Test
  public void testS3ResourceIdToString() {
    String filename = "s3://some-bucket/some/file.txt";
    S3ResourceId path = S3ResourceId.fromUri(filename);
    assertEquals(filename, path.toString());

    filename = "s3://some-bucket/some/";
    path = S3ResourceId.fromUri(filename);
    assertEquals(filename, path.toString());

    filename = "s3://some-bucket/";
    path = S3ResourceId.fromUri(filename);
    assertEquals(filename, path.toString());
  }

  @Test
  public void testEquals() {
    S3ResourceId a = S3ResourceId.fromComponents("s3", "bucket", "a/b/c");
    S3ResourceId b = S3ResourceId.fromComponents("s3", "bucket", "a/b/c");
    assertEquals(a, b);

    b = S3ResourceId.fromComponents("s3", a.getBucket(), "a/b/c/");
    assertNotEquals(a, b);

    b = S3ResourceId.fromComponents("s3", a.getBucket(), "x/y/z");
    assertNotEquals(a, b);

    b = S3ResourceId.fromComponents("s3", "other-bucket", a.getKey());
    assertNotEquals(a, b);
    assertNotEquals(b, a);

    b = S3ResourceId.fromComponents("other", "bucket", "a/b/c");
    assertNotEquals(a, b);
    assertNotEquals(b, a);
  }

  @Test
  public void testInvalidBucket() {
    thrown.expect(IllegalArgumentException.class);
    S3ResourceId.fromComponents("s3", "invalid/", "");
  }

  @Test
  public void testResourceIdTester() {
    S3Options options = PipelineOptionsFactory.create().as(S3Options.class);
    options.setAwsRegion("us-west-1");
    FileSystems.setDefaultPipelineOptions(options);
    ResourceIdTester.runResourceIdBattery(S3ResourceId.fromUri("s3://bucket/foo/"));
  }
}
