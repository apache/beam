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
package org.apache.beam.sdk.schemas.transforms.providers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StringCompilerTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  public static final String SQUARE_SOURCE =
      "import java.util.function.Function;"
          + "public class Square implements Function<Integer, Integer> {"
          + "  public Integer apply(Integer x) { return x * x; }"
          + "}";

  public static final String ROW_IDENTITY_SOURCE =
      "import java.util.function.Function;\n"
          + "import org.apache.beam.sdk.values.Row;\n"
          + "public class RowIdentity implements Function<Row, Row> {\n"
          + "  public Row apply(Row row) { return row; }\n"
          + "}";

  @Test
  public void testGetClass() throws Exception {
    Class<?> clazz = StringCompiler.getClass("Square", SQUARE_SOURCE);
    assertTrue(Function.class.isAssignableFrom(clazz));
    assertEquals("Square", clazz.getSimpleName());
  }

  @Test
  public void testGetInstance() throws Exception {
    Function<Integer, Integer> square =
        (Function<Integer, Integer>) StringCompiler.getInstance("Square", SQUARE_SOURCE);
    assertEquals(4, (int) square.apply(2));
  }

  @Test
  public void testGuessExpressionType() throws Exception {
    assertEquals(
        double.class,
        StringCompiler.guessExpressionType(
            "a+b", ImmutableMap.of("a", int.class, "b", double.class)));
    assertEquals(
        double.class,
        StringCompiler.guessExpressionType(
            "a > 0 ? a : b", ImmutableMap.of("a", int.class, "b", double.class)));
    assertEquals(
        double.class,
        StringCompiler.guessExpressionType("a * Math.random()", ImmutableMap.of("a", int.class)));
    assertEquals(
        int.class,
        StringCompiler.guessExpressionType("(int) a", ImmutableMap.of("a", double.class)));
    assertEquals(
        long.class,
        StringCompiler.guessExpressionType(
            "a.getInt64(\"foo\")+b", ImmutableMap.of("a", Row.class, "b", int.class)));
  }

  @Test
  public void testBuildClasspathFromPathingJar() throws Exception {
    File folder = tmp.newFolder("pathing_test");
    File dep1 = new File(folder, "dep1.jar");
    File dep2 = new File(folder, "dep2.jar");
    try (JarOutputStream os1 = new JarOutputStream(new FileOutputStream(dep1), new Manifest());
        JarOutputStream os2 = new JarOutputStream(new FileOutputStream(dep2), new Manifest())) {}

    Manifest pathingManifest = new Manifest();
    pathingManifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    // Mix file: URL with absolute path and relative path
    pathingManifest
        .getMainAttributes()
        .put(Attributes.Name.CLASS_PATH, "file:" + dep1.getAbsolutePath() + " " + dep2.getName());

    File pathingJar = new File(folder, "pathing.jar");
    try (JarOutputStream os =
        new JarOutputStream(new FileOutputStream(pathingJar), pathingManifest)) {}

    List<String> cp = StringCompiler.buildClasspath(null, pathingJar.getAbsolutePath());
    assertTrue("Should contain pathing jar", cp.contains(pathingJar.getAbsolutePath()));
    assertTrue("Should resolve dep1 via file: URL", cp.contains(dep1.getAbsolutePath()));
    assertTrue("Should resolve dep2 via relative path", cp.contains(dep2.getAbsolutePath()));
  }

  @Test
  public void testBuildClasspathIncludesRowAndStringCompiler() throws Exception {
    List<String> cp = StringCompiler.buildClasspath(null, "");
    String joined = String.join(File.pathSeparator, cp);
    // Code source of Row and StringCompiler must be included even with empty java.class.path
    URL rowLocation = Row.class.getProtectionDomain().getCodeSource().getLocation();
    URL compilerLocation = StringCompiler.class.getProtectionDomain().getCodeSource().getLocation();
    if (rowLocation != null) {
      File rowFile = new File(rowLocation.toURI());
      assertTrue(
          "Classpath should include Row.class location: " + rowFile.getAbsolutePath(),
          joined.contains(rowFile.getAbsolutePath()));
    }
    if (compilerLocation != null) {
      File compilerFile = new File(compilerLocation.toURI());
      assertTrue(
          "Classpath should include StringCompiler.class location: "
              + compilerFile.getAbsolutePath(),
          joined.contains(compilerFile.getAbsolutePath()));
    }
  }

  @Test
  public void testCompileRowResolvesRowEvenWhenNotInInitialClasspath() throws Exception {
    File folder = tmp.newFolder("isolated_cp");
    File dummyJar = new File(folder, "dummy.jar");
    try (JarOutputStream os =
        new JarOutputStream(new FileOutputStream(dummyJar), new Manifest())) {}

    // When classpath contains only dummy.jar, javac cannot resolve Row and throws CompileException
    assertThrows(
        StringCompiler.CompileException.class,
        () ->
            StringCompiler.getClass(
                "RowIdentity", ROW_IDENTITY_SOURCE, dummyJar.getAbsolutePath()));

    // When classpath is built using buildClasspath, Row.class is automatically resolved
    String expandedClasspath =
        String.join(
            File.pathSeparator, StringCompiler.buildClasspath(null, dummyJar.getAbsolutePath()));
    Class<?> clazz = StringCompiler.getClass("RowIdentity", ROW_IDENTITY_SOURCE, expandedClasspath);
    assertNotNull(clazz);
    Function<Row, Row> fn = (Function<Row, Row>) clazz.getDeclaredConstructor().newInstance();
    assertNotNull(fn);
  }

  @Test
  public void testResolveManifestEntryWithEncodedSpacesAndRelativePaths() {
    File parentDir = new File("/base/dir");
    // Percent-encoded relative path
    File f1 = StringCompiler.resolveManifestEntry(parentDir, "my%20lib.jar");
    assertEquals(new File(parentDir, "my lib.jar"), f1);

    // Percent-encoded nested relative path
    File f2 = StringCompiler.resolveManifestEntry(parentDir, "sub%20dir/dep%202.jar");
    assertEquals(new File(parentDir, "sub dir/dep 2.jar"), f2);

    // Percent-encoded file: URL
    File f3 = StringCompiler.resolveManifestEntry(parentDir, "file:/opt/beam/my%20lib.jar");
    assertEquals(new File("/opt/beam/my lib.jar"), f3);

    // Raw unencoded spaces fallback
    File f4 = StringCompiler.resolveManifestEntry(parentDir, "raw space.jar");
    assertEquals(new File(parentDir, "raw space.jar"), f4);

    // Invalid URI fallback
    File f5 = StringCompiler.resolveManifestEntry(parentDir, ":::invalid-uri:::");
    assertEquals(new File(parentDir, ":::invalid-uri:::"), f5);

    // Root-relative path without scheme
    File f6 = StringCompiler.resolveManifestEntry(parentDir, "/opt/beam/my%20lib.jar");
    assertEquals(new File("/opt/beam/my lib.jar"), f6);

    // Root-relative path with raw space
    File f7 = StringCompiler.resolveManifestEntry(parentDir, "/opt/beam/my lib.jar");
    assertEquals(new File("/opt/beam/my lib.jar"), f7);

    // Windows root-relative path with a single leading backslash (\opt\beam\my lib.jar)
    File fRootBackslash = StringCompiler.resolveManifestEntry(parentDir, "\\opt\\beam\\my lib.jar");
    assertEquals(new File("\\opt\\beam\\my lib.jar"), fRootBackslash);

    // Windows drive-letter absolute path with slash
    File f8 = StringCompiler.resolveManifestEntry(parentDir, "C:/opt/beam/my lib.jar");
    assertEquals(new File("C:/opt/beam/my lib.jar"), f8);

    // Windows drive-letter absolute path with encoded space
    File f9 = StringCompiler.resolveManifestEntry(parentDir, "C:/opt/beam/my%20lib.jar");
    assertEquals(new File("C:/opt/beam/my lib.jar"), f9);

    // Windows drive-letter absolute path with backslash
    File f10 = StringCompiler.resolveManifestEntry(parentDir, "C:\\opt\\beam\\my lib.jar");
    assertEquals(new File("C:\\opt\\beam\\my lib.jar"), f10);

    // Windows file: URL with drive letter
    File f11 = StringCompiler.resolveManifestEntry(parentDir, "file:/C:/opt/beam/my%20lib.jar");
    assertEquals(new File("C:/opt/beam/my lib.jar"), f11);

    // Windows drive-relative path without slash (e.g. C:folder/dep.jar) must NOT be treated as
    // absolute
    File f12 = StringCompiler.resolveManifestEntry(parentDir, "C:folder/dep.jar");
    assertEquals(new File(parentDir, "C:folder/dep.jar"), f12);

    // UNC path must NOT be resolved against parentDir
    File f13 = StringCompiler.resolveManifestEntry(parentDir, "\\\\server\\share\\dep.jar");
    assertEquals(new File("\\\\server\\share\\dep.jar"), f13);

    // Null and empty entry handling
    assertNull(StringCompiler.resolveManifestEntry(parentDir, null));
    assertNull(StringCompiler.resolveManifestEntry(parentDir, ""));

    // Null parentDir with relative entry
    File f14 = StringCompiler.resolveManifestEntry(null, "my%20lib.jar");
    assertEquals(new File("my lib.jar"), f14);
  }

  @Test
  public void testBuildClasspathWithPercentEncodedSpacesInManifest() throws Exception {
    File folder = tmp.newFolder("folder with spaces");
    File dep = new File(folder, "dep with spaces.jar");
    try (JarOutputStream os = new JarOutputStream(new FileOutputStream(dep), new Manifest())) {}

    Manifest pathingManifest = new Manifest();
    pathingManifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    // Class-Path attribute with percent-encoded relative URL per JAR spec
    pathingManifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, "dep%20with%20spaces.jar");

    File pathingJar = new File(folder, "pathing.jar");
    try (JarOutputStream os =
        new JarOutputStream(new FileOutputStream(pathingJar), pathingManifest)) {}

    List<String> cp = StringCompiler.buildClasspath(null, pathingJar.getAbsolutePath());
    assertTrue(
        "Should resolve percent-encoded dep with spaces", cp.contains(dep.getAbsolutePath()));
  }

  @Test
  public void testBuildClasspathURLClassLoaderNormalPath() throws Exception {
    File folder = tmp.newFolder("ucl_normal");
    File childJar = new File(folder, "child.jar");
    try (JarOutputStream os =
        new JarOutputStream(new FileOutputStream(childJar), new Manifest())) {}

    Manifest parentManifest = new Manifest();
    parentManifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    parentManifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, "child.jar");

    File parentJar = new File(folder, "parent.jar");
    try (JarOutputStream os =
        new JarOutputStream(new FileOutputStream(parentJar), parentManifest)) {}

    try (URLClassLoader ucl = new URLClassLoader(new URL[] {parentJar.toURI().toURL()})) {
      List<String> cp = StringCompiler.buildClasspath(ucl, "");
      assertTrue(
          "Should contain parent.jar from URLClassLoader",
          cp.contains(parentJar.getAbsolutePath()));
      assertTrue(
          "Should expand child.jar from manifest in URLClassLoader",
          cp.contains(childJar.getAbsolutePath()));
    }
  }

  @Test
  public void testBuildClasspathURLClassLoaderFallbackPath() throws Exception {
    // Create folder with space so that a raw file URL with unencoded space throws
    // URISyntaxException in url.toURI()
    File folder = tmp.newFolder("ucl fallback space");
    File childJar = new File(folder, "child.jar");
    try (JarOutputStream os =
        new JarOutputStream(new FileOutputStream(childJar), new Manifest())) {}

    Manifest parentManifest = new Manifest();
    parentManifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    parentManifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, "child.jar");

    File parentJar = new File(folder, "parent.jar");
    try (JarOutputStream os =
        new JarOutputStream(new FileOutputStream(parentJar), parentManifest)) {}

    // Construct a URL with unescaped space to intentionally trigger URISyntaxException in
    // url.toURI()
    URL unescapedUrl = new URL("file", "", parentJar.getAbsolutePath());
    assertThrows(Exception.class, unescapedUrl::toURI);

    try (URLClassLoader ucl = new URLClassLoader(new URL[] {unescapedUrl})) {
      List<String> cp = StringCompiler.buildClasspath(ucl, "");
      assertTrue(
          "Should contain parent.jar via fallback", cp.contains(parentJar.getAbsolutePath()));
      assertTrue(
          "Should expand child.jar from manifest via fallback",
          cp.contains(childJar.getAbsolutePath()));
    }
  }

  @Test
  public void testBuildClasspathCyclicManifestReferences() throws Exception {
    File folder = tmp.newFolder("cyclic_test");
    File cycleA = new File(folder, "cycleA.jar");
    File cycleB = new File(folder, "cycleB.jar");

    Manifest manifestA = new Manifest();
    manifestA.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifestA.getMainAttributes().put(Attributes.Name.CLASS_PATH, "cycleB.jar");

    Manifest manifestB = new Manifest();
    manifestB.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifestB.getMainAttributes().put(Attributes.Name.CLASS_PATH, "cycleA.jar");

    try (JarOutputStream osA = new JarOutputStream(new FileOutputStream(cycleA), manifestA);
        JarOutputStream osB = new JarOutputStream(new FileOutputStream(cycleB), manifestB)) {}

    List<String> cp = StringCompiler.buildClasspath(null, cycleA.getAbsolutePath());
    assertTrue("Should contain cycleA.jar", cp.contains(cycleA.getAbsolutePath()));
    assertTrue(
        "Should contain cycleB.jar without infinite loop", cp.contains(cycleB.getAbsolutePath()));
  }

  @Test
  public void testBuildClasspathDeduplication() throws Exception {
    File folder = tmp.newFolder("dedup_test");
    File dep = new File(folder, "dep.jar");
    try (JarOutputStream os = new JarOutputStream(new FileOutputStream(dep), new Manifest())) {}

    Manifest pathingManifest = new Manifest();
    pathingManifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    // Duplicate references in manifest
    pathingManifest
        .getMainAttributes()
        .put(Attributes.Name.CLASS_PATH, "dep.jar dep.jar file:" + dep.getAbsolutePath());

    File pathingJar = new File(folder, "pathing.jar");
    try (JarOutputStream os =
        new JarOutputStream(new FileOutputStream(pathingJar), pathingManifest)) {}

    // Pass duplicate in classPathProperty as well
    String classPathProp =
        pathingJar.getAbsolutePath()
            + File.pathSeparator
            + dep.getAbsolutePath()
            + File.pathSeparator
            + pathingJar.getAbsolutePath();

    List<String> cp = StringCompiler.buildClasspath(null, classPathProp);
    assertEquals(
        "dep.jar should appear exactly once in classpath",
        1,
        Collections.frequency(cp, dep.getAbsolutePath()));
    assertEquals(
        "pathing.jar should appear exactly once in classpath",
        1,
        Collections.frequency(cp, pathingJar.getAbsolutePath()));
  }
}
