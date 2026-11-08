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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
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
}
