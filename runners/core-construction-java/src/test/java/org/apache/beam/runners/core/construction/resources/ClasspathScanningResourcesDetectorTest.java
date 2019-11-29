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

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ClasspathScanningResourcesDetectorTest {

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public transient RestoreSystemProperties systemProperties = new RestoreSystemProperties();

  private ClasspathScanningResourcesDetector detector;

  @Before
  public void setUp() throws Exception {
    detector = new ClasspathScanningResourcesDetector();
  }

  @Test
  public void detectClassPathResourceWithFileResources() throws Exception {
    File file = tmpFolder.newFile("file");
    File file2 = tmpFolder.newFile("file2");
    URLClassLoader classLoader =
        new URLClassLoader(new URL[] {file.toURI().toURL(), file2.toURI().toURL()});

    assertEquals(
        ImmutableList.of(file.getAbsolutePath(), file2.getAbsolutePath()),
        detector.detect(classLoader));
  }

  @Test
  public void detectClassPathResourceFromJavaClassPathWhenTheresNoClassLoader() throws IOException {
    String path = tmpFolder.newFile("file").getAbsolutePath();
    String path2 = tmpFolder.newFile("file2").getAbsolutePath();
    String classpath = String.join(File.pathSeparator, path, path2);
    System.setProperty("java.class.path", classpath);

    List<String> resources = detector.detect(null);

    assertThat(resources, hasItems(path, path2));
    assertThat(resources, hasSize(2));
  }

  @Test
  public void throwWhenDetectingClassPathResourceWithNonFileResources() throws Exception {
    String url = "http://www.google.com/all-the-secrets.jar";
    URLClassLoader classLoader = new URLClassLoader(new URL[] {new URL(url)});

    IllegalArgumentException exeption =
        assertThrows(IllegalArgumentException.class, () -> detector.detect(classLoader));

    assertEquals("Unable to convert url (" + url + ") to file.", exeption.getMessage());
  }
}
