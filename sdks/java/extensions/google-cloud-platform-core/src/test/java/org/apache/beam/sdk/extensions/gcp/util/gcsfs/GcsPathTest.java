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
package org.apache.beam.sdk.extensions.gcp.util.gcsfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of GcsPath. */
@RunWith(JUnit4.class)
public class GcsPathTest {

  /** Test case, which tests parsing and building of GcsPaths. */
  static final class TestCase {

    final String uri;
    final String expectedBucket;
    final String expectedObject;
    final String[] namedComponents;

    TestCase(String uri, String... namedComponents) {
      this.uri = uri;
      this.expectedBucket = namedComponents[0];
      this.namedComponents = namedComponents;
      this.expectedObject = uri.substring(expectedBucket.length() + 6);
    }
  }

  // Each test case is an expected URL, then the components used to build it.
  // Empty components result in a double slash.
  static final List<TestCase> PATH_TEST_CASES =
      Arrays.asList(
          new TestCase("gs://bucket/then/object", "bucket", "then", "object"),
          new TestCase("gs://bucket//then/object", "bucket", "", "then", "object"),
          new TestCase("gs://bucket/then//object", "bucket", "then", "", "object"),
          new TestCase("gs://bucket/then///object", "bucket", "then", "", "", "object"),
          new TestCase("gs://bucket/then/object/", "bucket", "then", "object/"),
          new TestCase("gs://bucket/then/object/", "bucket", "then/", "object/"),
          new TestCase("gs://bucket/then/object//", "bucket", "then", "object", ""),
          new TestCase("gs://bucket/then/object//", "bucket", "then", "object/", ""),
          new TestCase("gs://bucket/", "bucket"));

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGcsPathParsing() throws Exception {
    for (TestCase testCase : PATH_TEST_CASES) {
      String uriString = testCase.uri;

      GcsPath path = GcsPath.fromUri(URI.create(uriString));
      // Deconstruction - check bucket, object, and components.
      assertEquals(testCase.expectedBucket, path.getBucket());
      assertEquals(testCase.expectedObject, path.getObject());
      assertEquals(testCase.uri, testCase.namedComponents.length, path.getNameCount());

      // Construction - check that the path can be built from components.
      GcsPath built = GcsPath.fromComponents(null, null);
      for (String component : testCase.namedComponents) {
        built = built.resolve(component);
      }
      assertEquals(testCase.uri, built.toString());
    }
  }

  @Test
  public void testParentRelationship() throws Exception {
    GcsPath path = GcsPath.fromComponents("bucket", "then/object");
    assertEquals("bucket", path.getBucket());
    assertEquals("then/object", path.getObject());
    assertEquals(3, path.getNameCount());
    assertTrue(path.endsWith("object"));
    assertTrue(path.startsWith("bucket/then"));

    GcsPath parent = path.getParent(); // gs://bucket/then/
    assertEquals("bucket", parent.getBucket());
    assertEquals("then/", parent.getObject());
    assertEquals(2, parent.getNameCount());
    assertThat(path, Matchers.not(Matchers.equalTo(parent)));
    assertTrue(path.startsWith(parent));
    assertFalse(parent.startsWith(path));
    assertTrue(parent.endsWith("then/"));
    assertTrue(parent.startsWith("bucket/then"));
    assertTrue(parent.isAbsolute());

    GcsPath root = path.getRoot();
    assertEquals(0, root.getNameCount());
    assertEquals("gs://", root.toString());
    assertEquals("", root.getBucket());
    assertEquals("", root.getObject());
    assertTrue(root.isAbsolute());
    assertThat(root, Matchers.equalTo(parent.getRoot()));

    GcsPath grandParent = parent.getParent(); // gs://bucket/
    assertEquals(1, grandParent.getNameCount());
    assertEquals("gs://bucket/", grandParent.toString());
    assertTrue(grandParent.isAbsolute());
    assertThat(root, Matchers.equalTo(grandParent.getParent()));
    assertThat(root.getParent(), Matchers.nullValue());

    assertTrue(path.startsWith(path.getRoot()));
    assertTrue(parent.startsWith(path.getRoot()));
  }

  @Test
  public void testRelativeParent() throws Exception {
    GcsPath path = GcsPath.fromComponents(null, "a/b");
    GcsPath parent = path.getParent();
    assertEquals("a/", parent.toString());

    GcsPath grandParent = parent.getParent();
    assertNull(grandParent);
  }

  @Test
  public void testUriSupport() throws Exception {
    URI uri = URI.create("gs://bucket/some/path");

    GcsPath path = GcsPath.fromUri(uri);
    assertEquals("bucket", path.getBucket());
    assertEquals("some/path", path.getObject());

    URI reconstructed = path.toUri();
    assertEquals(uri, reconstructed);

    path = GcsPath.fromUri("gs://bucket");
    assertEquals("gs://bucket/", path.toString());
  }

  @Test
  public void testBucketParsing() throws Exception {
    GcsPath path = GcsPath.fromUri("gs://bucket");
    GcsPath path2 = GcsPath.fromUri("gs://bucket/");

    assertEquals(path, path2);
    assertEquals(path.toString(), path2.toString());
    assertEquals(path.toUri(), path2.toUri());
  }

  @Test
  public void testGcsPathToString() throws Exception {
    String filename = "gs://some_bucket/some/file.txt";
    GcsPath path = GcsPath.fromUri(filename);
    assertEquals(filename, path.toString());
  }

  @Test
  public void testGcsPath_withGeneration() {
    String filename = "gs://some_bucket/some/file.txt#12345";
    String actualFilename = "gs://some_bucket/some/file.txt";
    GcsPath path = GcsPath.fromUri(filename);
    assertEquals(Long.valueOf(12345), path.getGeneration());
    assertEquals(path.toString(), actualFilename);
  }

  @Test
  public void testGcsPath_withoutGeneration_generationNull() {
    String filename = "gs://some_bucket/some/file.txt";
    GcsPath path = GcsPath.fromUri(filename);
    assertNull(path.getGeneration());
  }

  @Test
  public void testGcsPath_withPoundSignInObjectName() {
    String filename = "gs://some_bucket/some/directory #55/file.txt";
    GcsPath path = GcsPath.fromUri(filename);
    assertNull(path.getGeneration());
    assertEquals(path.toString(), filename);
  }

  @Test
  public void testGcsPath_withPoundSignInObjectName_withGeneration() {
    String filename = "gs://some_bucket/some/directory #55/file.txt#12345";
    String actualFilename = "gs://some_bucket/some/directory #55/file.txt";
    GcsPath path = GcsPath.fromUri(filename);
    assertEquals(Long.valueOf(12345), path.getGeneration());
    assertEquals(path.toString(), actualFilename);
  }

  @Test
  public void testEquals() {
    GcsPath a = GcsPath.fromComponents(null, "a/b/c");
    GcsPath a2 = GcsPath.fromComponents(null, "a/b/c");
    assertFalse(a.isAbsolute());
    assertFalse(a2.isAbsolute());

    GcsPath b = GcsPath.fromComponents("bucket", "a/b/c");
    GcsPath b2 = GcsPath.fromComponents("bucket", "a/b/c");
    assertTrue(b.isAbsolute());
    assertTrue(b2.isAbsolute());

    assertEquals(a, a);
    assertThat(a, Matchers.not(Matchers.equalTo(b)));
    assertThat(b, Matchers.not(Matchers.equalTo(a)));

    assertEquals(a, a2);
    assertEquals(a2, a);
    assertEquals(b, b2);
    assertEquals(b2, b);

    assertThat(a, Matchers.not(Matchers.equalTo(Paths.get("/tmp/foo"))));
    assertNotNull(a);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidGcsPath() {
    @SuppressWarnings("unused")
    GcsPath filename = GcsPath.fromUri("file://invalid/gcs/path");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBucket() {
    GcsPath.fromComponents("invalid/", "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidObject_newline() {
    GcsPath.fromComponents(null, "a\nb");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidObject_cr() {
    GcsPath.fromComponents(null, "a\rb");
  }

  @Test
  public void testResolveUri() {
    GcsPath path = GcsPath.fromComponents("bucket", "a/b/c");
    GcsPath d = path.resolve("gs://bucket2/d");
    assertEquals("gs://bucket2/d", d.toString());
  }

  @Test
  public void testResolveOther() {
    GcsPath a = GcsPath.fromComponents("bucket", "a");
    GcsPath b = a.resolve(Paths.get("b"));
    assertEquals("a/b", b.getObject());
  }

  @Test
  public void testGetFileName() {
    assertEquals("foo", GcsPath.fromUri("gs://bucket/bar/foo").getFileName().toString());
    assertEquals("foo", GcsPath.fromUri("gs://bucket/foo").getFileName().toString());
    thrown.expect(UnsupportedOperationException.class);
    GcsPath.fromUri("gs://bucket/").getFileName();
  }

  @Test
  public void testResolveSibling() {
    assertEquals("gs://bucket/bar/moo", GcsPath.fromUri("gs://bucket/bar/foo").resolveSibling("moo").toString());
    assertEquals("gs://bucket/moo", GcsPath.fromUri("gs://bucket/foo").resolveSibling("moo").toString());
    thrown.expect(UnsupportedOperationException.class);
    GcsPath.fromUri("gs://bucket/").resolveSibling("moo");
  }

  @Test
  public void testCompareTo() {
    GcsPath a = GcsPath.fromComponents("bucket", "a");
    GcsPath b = GcsPath.fromComponents("bucket", "b");
    GcsPath b2 = GcsPath.fromComponents("bucket2", "b");
    GcsPath brel = GcsPath.fromComponents(null, "b");
    GcsPath a2 = GcsPath.fromComponents("bucket", "a");
    GcsPath arel = GcsPath.fromComponents(null, "a");

    assertThat(a.compareTo(b), Matchers.lessThan(0));
    assertThat(b.compareTo(a), Matchers.greaterThan(0));
    assertThat(a.compareTo(a2), Matchers.equalTo(0));

    assertThat(a.hashCode(), Matchers.equalTo(a2.hashCode()));
    assertThat(a.hashCode(), Matchers.not(Matchers.equalTo(b.hashCode())));
    assertThat(b.hashCode(), Matchers.not(Matchers.equalTo(brel.hashCode())));

    assertThat(brel.compareTo(b), Matchers.lessThan(0));
    assertThat(b.compareTo(brel), Matchers.greaterThan(0));
    assertThat(arel.compareTo(brel), Matchers.lessThan(0));
    assertThat(brel.compareTo(arel), Matchers.greaterThan(0));

    assertThat(b.compareTo(b2), Matchers.lessThan(0));
    assertThat(b2.compareTo(b), Matchers.greaterThan(0));
  }

  @Test
  public void testCompareTo_ordering() {
    GcsPath ab = GcsPath.fromComponents("bucket", "a/b");
    GcsPath abc = GcsPath.fromComponents("bucket", "a/b/c");
    GcsPath a1b = GcsPath.fromComponents("bucket", "a-1/b");

    assertThat(ab.compareTo(a1b), Matchers.lessThan(0));
    assertThat(a1b.compareTo(ab), Matchers.greaterThan(0));

    assertThat(ab.compareTo(abc), Matchers.lessThan(0));
    assertThat(abc.compareTo(ab), Matchers.greaterThan(0));
  }

  @Test
  public void testCompareTo_buckets() {
    GcsPath a = GcsPath.fromComponents(null, "a/b/c");
    GcsPath b = GcsPath.fromComponents("bucket", "a/b/c");

    assertThat(a.compareTo(b), Matchers.lessThan(0));
    assertThat(b.compareTo(a), Matchers.greaterThan(0));
  }

  @Test
  public void testIterator() {
    GcsPath a = GcsPath.fromComponents("bucket", "a/b/c");
    Iterator<Path> it = a.iterator();

    assertTrue(it.hasNext());
    assertEquals("gs://bucket/", it.next().toString());
    assertTrue(it.hasNext());
    assertEquals("a", it.next().toString());
    assertTrue(it.hasNext());
    assertEquals("b", it.next().toString());
    assertTrue(it.hasNext());
    assertEquals("c", it.next().toString());
    assertFalse(it.hasNext());
  }

  @Test
  public void testSubpath() {
    GcsPath a = GcsPath.fromComponents("bucket", "a/b/c/d");
    assertThat(a.subpath(0, 1).toString(), Matchers.equalTo("gs://bucket/"));
    assertThat(a.subpath(0, 2).toString(), Matchers.equalTo("gs://bucket/a"));
    assertThat(a.subpath(0, 3).toString(), Matchers.equalTo("gs://bucket/a/b"));
    assertThat(a.subpath(0, 4).toString(), Matchers.equalTo("gs://bucket/a/b/c"));
    assertThat(a.subpath(1, 2).toString(), Matchers.equalTo("a"));
    assertThat(a.subpath(2, 3).toString(), Matchers.equalTo("b"));
    assertThat(a.subpath(2, 4).toString(), Matchers.equalTo("b/c"));
    assertThat(a.subpath(2, 5).toString(), Matchers.equalTo("b/c/d"));
  }

  @Test
  public void testGetName() {
    GcsPath a = GcsPath.fromComponents("bucket", "a/b/c/d");
    assertEquals(5, a.getNameCount());
    assertThat(a.getName(0).toString(), Matchers.equalTo("gs://bucket/"));
    assertThat(a.getName(1).toString(), Matchers.equalTo("a"));
    assertThat(a.getName(2).toString(), Matchers.equalTo("b"));
    assertThat(a.getName(3).toString(), Matchers.equalTo("c"));
    assertThat(a.getName(4).toString(), Matchers.equalTo("d"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSubPathError() {
    GcsPath a = GcsPath.fromComponents("bucket", "a/b/c/d");
    a.subpath(1, 1); // throws IllegalArgumentException
    Assert.fail();
  }
}
