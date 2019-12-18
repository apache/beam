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
package org.apache.beam.runners.core.construction.resources;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class ClasspathScanningResourcesDetectorTest {

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public transient RestoreSystemProperties systemProperties = new RestoreSystemProperties();

  private ClasspathScanningResourcesDetector detector;

  private ClassLoader classLoader;

  @Before
  public void setUp() {
    detector = new ClasspathScanningResourcesDetector(new ClassGraph());
  }

  @Test
  public void shouldDetectDirectories() throws Exception {
    File folder = tmpFolder.newFolder("folder1");
    classLoader = new URLClassLoader(new URL[] {folder.toURI().toURL()});

    List<String> result = detector.detect(classLoader);

    assertThat(result, hasItem(containsString(folder.getAbsolutePath())));
  }

  @Test
  public void shouldDetectJarFiles() throws Exception {
    File jarFile = createTestTmpJarFile("test");
    classLoader = new URLClassLoader(new URL[] {jarFile.toURI().toURL()});

    List<String> result = detector.detect(classLoader);

    assertThat(result, hasItem(containsString(jarFile.getAbsolutePath())));
  }

  private File createTestTmpJarFile(String name) throws IOException {
    File jarFile = tmpFolder.newFile(name);
    try (JarOutputStream os = new JarOutputStream(new FileOutputStream(jarFile), new Manifest())) {}
    return jarFile;
  }

  @Test
  public void shouldNotDetectOrdinaryFiles() throws Exception {
    File textFile = tmpFolder.newFile("ordinaryTextFile.txt");
    classLoader = new URLClassLoader(new URL[] {textFile.toURI().toURL()});

    List<String> result = detector.detect(classLoader);

    assertThat(result, not(hasItem(containsString(textFile.getAbsolutePath()))));
  }

  @Test
  public void shouldDetectClassPathResourceFromJavaClassPathEnvVariable() throws IOException {
    String path = tmpFolder.newFolder("folder").getAbsolutePath();
    System.setProperty("java.class.path", path);

    List<String> resources = detector.detect(null);

    assertThat(resources, hasItems(containsString(path)));
  }

  @Test
  public void shouldNotDetectClassPathResourceThatIsNotAFile() throws Exception {
    String url = "http://www.google.com/all-the-secrets.jar";
    classLoader = new URLClassLoader(new URL[] {new URL(url)});

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

    List<String> detectedResources = detector.detect(uselessClassLoader);

    assertFalse(detectedResources.isEmpty());
  }
}
