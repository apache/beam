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

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.options.FileStagingOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for PipelineResources. */
@RunWith(JUnit4.class)
public class PipelineResourcesTest {

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testDetectsResourcesToStage() throws IOException {
    File file = tmpFolder.newFile("file");
    URLClassLoader classLoader = new URLClassLoader(new URL[] {file.toURI().toURL()});
    PipelineResourcesOptions options =
        PipelineOptionsFactory.create().as(PipelineResourcesOptions.class);

    List<String> detectedResources =
        PipelineResources.detectClassPathResourcesToStage(classLoader, options);

    assertThat(detectedResources, not(empty()));
  }

  @Test
  public void testDetectedResourcesListDoNotContainNotStageableResources() throws IOException {
    File unstageableResource = tmpFolder.newFolder(".gradle/wrapper/unstageableResource");
    URLClassLoader classLoader =
        new URLClassLoader(new URL[] {unstageableResource.toURI().toURL()});
    PipelineResourcesOptions options =
        PipelineOptionsFactory.create().as(PipelineResourcesOptions.class);

    List<String> detectedResources =
        PipelineResources.detectClassPathResourcesToStage(classLoader, options);

    assertThat(detectedResources, not(contains(unstageableResource.getAbsolutePath())));
  }

  @Test
  public void testFailOnNonExistingPaths() throws IOException {
    String nonexistentFilePath = tmpFolder.getRoot().getPath() + "/nonexistent/file";
    String existingFilePath = tmpFolder.newFile("existingFile").getAbsolutePath();
    List<String> filesToStage = Arrays.asList(nonexistentFilePath, existingFilePath);
    String temporaryLocation = tmpFolder.newFolder().getAbsolutePath();

    assertThrows(
        "To-be-staged file does not exist: ",
        IllegalStateException.class,
        () -> PipelineResources.prepareFilesForStaging(filesToStage, temporaryLocation));
  }

  @Test
  public void testPackagingDirectoryResourceToJarFile() throws IOException {
    String directoryPath = tmpFolder.newFolder().getAbsolutePath();
    List<String> filesToStage = Arrays.asList(directoryPath);
    String temporaryLocation = tmpFolder.newFolder().getAbsolutePath();

    List<String> result = PipelineResources.prepareFilesForStaging(filesToStage, temporaryLocation);

    assertTrue(new File(result.get(0)).exists());
    assertTrue(result.get(0).matches(".*\\.jar"));
  }

  @Test
  public void testIfThrowsWhenThereIsNoTemporaryFolderForJars() throws IOException {
    List<String> filesToStage = Arrays.asList(tmpFolder.newFolder().getAbsolutePath());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> PipelineResources.prepareFilesForStaging(filesToStage, null));

    assertEquals(
        "Please provide temporary location for storing the jar files.", exception.getMessage());
  }

  @Test
  public void testPrepareFilesForStagingFromOptions() throws IOException {
    String nonexistentFilePath = tmpFolder.getRoot().getPath() + "/nonexistent/file";
    String existingFilePath = tmpFolder.newFile("existingFile").getAbsolutePath();
    List<String> filesToStage = Arrays.asList(nonexistentFilePath, existingFilePath);
    String temporaryLocation = tmpFolder.newFolder().getAbsolutePath();

    FileStagingOptions options = PipelineOptionsFactory.create().as(FileStagingOptions.class);
    options.setFilesToStage(filesToStage);
    options.setTempLocation(temporaryLocation);

    assertThrows(
        "To-be-staged file does not exist: ",
        IllegalStateException.class,
        () -> PipelineResources.prepareFilesForStaging(filesToStage, temporaryLocation));
  }

  @Test
  public void testPackagingDirectoryResourceFromOptions() throws IOException {
    String directoryPath = tmpFolder.newFolder().getAbsolutePath();
    List<String> filesToStage = Arrays.asList(directoryPath);
    String temporaryLocation = tmpFolder.newFolder().getAbsolutePath();

    FileStagingOptions options = PipelineOptionsFactory.create().as(FileStagingOptions.class);
    options.setFilesToStage(filesToStage);
    options.setTempLocation(temporaryLocation);

    PipelineResources.prepareFilesForStaging(options);
    List<String> result = options.getFilesToStage();

    assertEquals(1, result.size());
    assertTrue(new File(result.get(0)).exists());
    assertTrue(result.get(0).matches(".*\\.jar"));
  }

  @Test
  public void testIfThrowsWhenThereIsNoTemporaryFolderForJarsFromOptions() throws IOException {
    List<String> filesToStage = Arrays.asList(tmpFolder.newFolder().getAbsolutePath());

    FileStagingOptions options = PipelineOptionsFactory.create().as(FileStagingOptions.class);
    options.setFilesToStage(filesToStage);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> PipelineResources.prepareFilesForStaging(filesToStage, null));

    assertEquals(
        "Please provide temporary location for storing the jar files.", exception.getMessage());
  }

  @Test
  public void testPrepareFilesForStagingUndefinedFilesToStage() throws IOException {
    String temporaryLocation = tmpFolder.newFolder().getAbsolutePath();

    FileStagingOptions options = PipelineOptionsFactory.create().as(FileStagingOptions.class);
    options.setTempLocation(temporaryLocation);

    PipelineResources.prepareFilesForStaging(options);
    List<String> result = options.getFilesToStage();

    assertNotNull(result);
    assertTrue(result.size() > 0);
  }
}
