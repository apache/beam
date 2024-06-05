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
package org.apache.beam.sdk.util.construction.resources;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;

import io.github.classgraph.ClassGraph;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class ClasspathScanningResourcesDetectorTest {

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public transient RestoreSystemProperties systemProperties = new RestoreSystemProperties();

  @Test
  public void shouldDetectDirectories() throws Exception {
    File folder = tmpFolder.newFolder("folder1");
    ClassLoader classLoader = new URLClassLoader(new URL[] {folder.toURI().toURL()});
    ClasspathScanningResourcesDetector detector =
        new ClasspathScanningResourcesDetector(new ClassGraph());

    List<String> result = detector.detect(classLoader);

    assertThat(result, hasItem(containsString(folder.getCanonicalPath())));
  }

  @Test
  public void shouldDetectJarFiles() throws Exception {
    File jarFile = createTestTmpJarFile("test");
    ClassLoader classLoader = new URLClassLoader(new URL[] {jarFile.toURI().toURL()});
    ClasspathScanningResourcesDetector detector =
        new ClasspathScanningResourcesDetector(new ClassGraph());

    List<String> result = detector.detect(classLoader);

    assertThat(result, hasItem(containsString(jarFile.getCanonicalPath())));
  }

  @Test
  public void shouldDetectResourcesInOrderTheyAppearInURLClassLoader() throws Exception {
    File file1 = createTestTmpJarFile("test1");
    File file2 = createTestTmpJarFile("test2");
    ClassLoader classLoader =
        new URLClassLoader(new URL[] {file1.toURI().toURL(), file2.toURI().toURL()});

    ClasspathScanningResourcesDetector detector =
        new ClasspathScanningResourcesDetector(new ClassGraph());

    List<String> result = detector.detect(classLoader);

    assertThat(
        result,
        containsInRelativeOrder(
            containsString(file1.getCanonicalPath()), containsString(file2.getCanonicalPath())));
  }

  private File createTestTmpJarFile(String name) throws IOException {
    File jarFile = tmpFolder.newFile(name);
    try (JarOutputStream os = new JarOutputStream(new FileOutputStream(jarFile), new Manifest())) {}
    return jarFile;
  }

  @Test
  public void shouldNotDetectOrdinaryFiles() throws Exception {
    File textFile = tmpFolder.newFile("ordinaryTextFile.txt");
    ClassLoader classLoader = new URLClassLoader(new URL[] {textFile.toURI().toURL()});
    ClasspathScanningResourcesDetector detector =
        new ClasspathScanningResourcesDetector(new ClassGraph());

    List<String> result = detector.detect(classLoader);

    assertThat(result, not(hasItem(containsString(textFile.getCanonicalPath()))));
  }

  @Test
  public void shouldDetectClassPathResourceFromJavaClassPathEnvVariable() throws IOException {
    String path = tmpFolder.newFolder("folder").getCanonicalPath();
    System.setProperty("java.class.path", path);
    ClasspathScanningResourcesDetector detector =
        new ClasspathScanningResourcesDetector(new ClassGraph());

    List<String> resources = detector.detect(null);

    assertThat(resources, hasItems(containsString(path)));
  }

  @Test
  public void shouldNotDetectClassPathResourceThatIsNotAFile() throws Exception {
    String url = "http://www.example.com/all-the-secrets.jar";
    ClassLoader classLoader = new URLClassLoader(new URL[] {new URL(url)});
    ClasspathScanningResourcesDetector detector =
        new ClasspathScanningResourcesDetector(new ClassGraph());

    List<String> result = detector.detect(classLoader);

    assertThat(result, not(hasItem(containsString(url))));
  }

  /*
   * ClassGraph library that is used in the tested algorithm can still detect resources from
   * "java.class.path" env variable. Even in case the classloader that is passed is of no use we
   * will still be able to detect and load resource paths from the env variable.
   */
  @Test
  public void shouldStillDetectResourcesEvenIfClassloaderIsUseless() {
    ClassLoader uselessClassLoader = Mockito.mock(ClassLoader.class);
    ClasspathScanningResourcesDetector detector =
        new ClasspathScanningResourcesDetector(new ClassGraph());

    List<String> detectedResources = detector.detect(uselessClassLoader);

    assertFalse(detectedResources.isEmpty());
  }
}
