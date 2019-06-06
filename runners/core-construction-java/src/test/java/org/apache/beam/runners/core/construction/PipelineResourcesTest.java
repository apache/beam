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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for PipelineResources. */
@RunWith(JUnit4.class)
public class PipelineResourcesTest {

  private static final String classPath = System.getProperty("java.class.path");

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @AfterClass
  public static void cleanup() {
    System.setProperty("java.class.path", classPath);
  }

  @Test
  public void detectClassPathResourceWithFileResources() throws Exception {
    File file = tmpFolder.newFile("file");
    File file2 = tmpFolder.newFile("file2");
    System.setProperty(
        "java.class.path",
        String.join(File.pathSeparator, file.getAbsolutePath(), file2.getAbsolutePath()));

    assertEquals(
        ImmutableList.of(file.getAbsolutePath(), file2.getAbsolutePath()),
        PipelineResources.detectClassPathResourcesToStage());
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
