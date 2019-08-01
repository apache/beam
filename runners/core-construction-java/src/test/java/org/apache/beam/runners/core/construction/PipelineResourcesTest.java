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
package org.apache.beam.runners.core.construction;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for PipelineResources. */
@RunWith(JUnit4.class)
public class PipelineResourcesTest {

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void detectClassPathResourceWithFileResources() throws Exception {
    File file = tmpFolder.newFile("file");
    File file2 = tmpFolder.newFile("file2");
    URLClassLoader classLoader =
        new URLClassLoader(new URL[] {file.toURI().toURL(), file2.toURI().toURL()});

    assertEquals(
        ImmutableList.of(file.getAbsolutePath(), file2.getAbsolutePath()),
        PipelineResources.detectClassPathResourcesToStage(classLoader));
  }

  @Test
  public void detectClassPathResourcesWithUnsupportedClassLoader() {
    ClassLoader mockClassLoader = Mockito.mock(ClassLoader.class);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unable to use ClassLoader to detect classpath elements.");

    PipelineResources.detectClassPathResourcesToStage(mockClassLoader);
  }

  @Test
  public void detectClassPathResourceWithNonFileResources() throws Exception {
    String url = "http://www.google.com/all-the-secrets.jar";
    URLClassLoader classLoader = new URLClassLoader(new URL[] {new URL(url)});
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unable to convert url (" + url + ") to file.");

    PipelineResources.detectClassPathResourcesToStage(classLoader);
  }

  @Test
  public void testFailOnNonExistingPaths() throws IOException {
    String nonexistentFilePath = tmpFolder.getRoot().getPath() + "/nonexistent/file";
    String existingFilePath = tmpFolder.newFile("existingFile").getAbsolutePath();
    String temporaryLocation = tmpFolder.newFolder().getAbsolutePath();

    List<String> filesToStage = Arrays.asList(nonexistentFilePath, existingFilePath);

    assertThrows(
        "To-be-staged file does not exist: ",
        IllegalStateException.class,
        () -> PipelineResources.prepareFilesForStaging(filesToStage, temporaryLocation));
  }

  @Test
  public void testPackagingDirectoryResourceToJarFile() throws IOException {
    String directoryPath = tmpFolder.newFolder().getAbsolutePath();
    String temporaryLocation = tmpFolder.newFolder().getAbsolutePath();

    List<String> filesToStage = new ArrayList<>();
    filesToStage.add(directoryPath);

    List<String> result = PipelineResources.prepareFilesForStaging(filesToStage, temporaryLocation);

    assertTrue(new File(result.get(0)).exists());
    assertTrue(result.get(0).matches(".*\\.jar"));
  }

  @Test
  public void testIfThrowsWhenThereIsNoTemporaryFolderForJars() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Please provide temporary location for storing the jar files.");

    List<String> filesToStage = new ArrayList<>();
    filesToStage.add(tmpFolder.newFolder().getAbsolutePath());

    PipelineResources.prepareFilesForStaging(filesToStage, null);
  }
}
