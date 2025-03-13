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
package org.apache.beam.sdk.transformservice.launcher;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TransformServiceLauncherTest {

  @Test
  public void testLauncherCreatesCredentialsDir() throws IOException {
    String projectName = UUID.randomUUID().toString();
    Path expectedTempDir = Paths.get(System.getProperty("java.io.tmpdir"), projectName);
    File file = expectedTempDir.toFile();
    file.deleteOnExit();
    TransformServiceLauncher.forProject(projectName, 12345, null);
    Path expectedCredentialsDir = Paths.get(expectedTempDir.toString(), "credentials_dir");
    Assert.assertTrue(expectedCredentialsDir.toFile().exists());
  }

  @Test
  public void testLauncherCreatesDependenciesDir() throws IOException {
    String projectName = UUID.randomUUID().toString();
    Path expectedTempDir = Paths.get(System.getProperty("java.io.tmpdir"), projectName);
    File file = expectedTempDir.toFile();
    file.deleteOnExit();
    TransformServiceLauncher.forProject(projectName, 12345, null);
    Path expectedCredentialsDir = Paths.get(expectedTempDir.toString(), "dependencies_dir");
    Assert.assertTrue(expectedCredentialsDir.toFile().exists());
  }

  @Test
  public void testLauncherInstallsDependencies() throws IOException {
    String projectName = UUID.randomUUID().toString();
    Path expectedTempDir = Paths.get(System.getProperty("java.io.tmpdir"), projectName);
    File file = expectedTempDir.toFile();
    file.deleteOnExit();

    File requirementsFile =
        Paths.get(
                System.getProperty("java.io.tmpdir"),
                ("requirements" + UUID.randomUUID().toString() + ".txt"))
            .toFile();
    requirementsFile.deleteOnExit();

    try (Writer fout =
        new OutputStreamWriter(
            new FileOutputStream(requirementsFile.getAbsolutePath()), StandardCharsets.UTF_8)) {
      fout.write("pypipackage1\n");
      fout.write("pypipackage2\n");
    }

    TransformServiceLauncher.forProject(projectName, 12345, requirementsFile.getAbsolutePath());

    // Confirming that the Transform Service launcher created a temporary requirements file with the
    // specified set of packages.
    Path expectedUpdatedRequirementsFile =
        Paths.get(expectedTempDir.toString(), "dependencies_dir", "requirements.txt");
    Assert.assertTrue(expectedUpdatedRequirementsFile.toFile().exists());

    ArrayList<String> expectedUpdatedRequirementsFileLines = new ArrayList<>();
    try (BufferedReader bufReader =
        Files.newBufferedReader(expectedUpdatedRequirementsFile, UTF_8)) {
      String line = bufReader.readLine();
      while (line != null) {
        expectedUpdatedRequirementsFileLines.add(line);
        line = bufReader.readLine();
      }
    }

    Assert.assertEquals(2, expectedUpdatedRequirementsFileLines.size());
    Assert.assertTrue(expectedUpdatedRequirementsFileLines.contains("pypipackage1"));
    Assert.assertTrue(expectedUpdatedRequirementsFileLines.contains("pypipackage2"));
  }

  @Test
  public void testLauncherInstallsLocalDependencies() throws IOException {
    String projectName = UUID.randomUUID().toString();
    Path expectedTempDir = Paths.get(System.getProperty("java.io.tmpdir"), projectName);
    File file = expectedTempDir.toFile();
    file.deleteOnExit();

    String dependency1FileName = "dep_" + UUID.randomUUID().toString();
    File dependency1 =
        Paths.get(System.getProperty("java.io.tmpdir"), dependency1FileName).toFile();
    dependency1.deleteOnExit();
    try (Writer fout =
        new OutputStreamWriter(
            new FileOutputStream(dependency1.getAbsolutePath()), StandardCharsets.UTF_8)) {
      fout.write("tempdata\n");
    }

    String dependency2FileName = "dep_" + UUID.randomUUID().toString();
    File dependency2 =
        Paths.get(System.getProperty("java.io.tmpdir"), dependency2FileName).toFile();
    dependency2.deleteOnExit();
    try (Writer fout =
        new OutputStreamWriter(
            new FileOutputStream(dependency2.getAbsolutePath()), StandardCharsets.UTF_8)) {
      fout.write("tempdata\n");
    }

    File requirementsFile =
        Paths.get(
                System.getProperty("java.io.tmpdir"),
                ("requirements" + UUID.randomUUID().toString() + ".txt"))
            .toFile();
    requirementsFile.deleteOnExit();
    try (Writer fout =
        new OutputStreamWriter(
            new FileOutputStream(requirementsFile.getAbsolutePath()), StandardCharsets.UTF_8)) {
      fout.write(dependency1.getAbsolutePath() + "\n");
      fout.write(dependency2.getAbsolutePath() + "\n");
      fout.write("pypipackage" + "\n");
    }

    TransformServiceLauncher.forProject(projectName, 12345, requirementsFile.getAbsolutePath());

    // Confirming that the Transform Service launcher created a temporary requirements file with the
    // specified set of packages.
    Path expectedUpdatedRequirementsFile =
        Paths.get(expectedTempDir.toString(), "dependencies_dir", "requirements.txt");
    Assert.assertTrue(expectedUpdatedRequirementsFile.toFile().exists());

    ArrayList<String> expectedUpdatedRequirementsFileLines = new ArrayList<>();
    try (BufferedReader bufReader =
        Files.newBufferedReader(expectedUpdatedRequirementsFile, UTF_8)) {
      String line = bufReader.readLine();
      while (line != null) {
        expectedUpdatedRequirementsFileLines.add(line);
        line = bufReader.readLine();
      }
    }

    // To make local packages available to the expansion service Docker containers, the temporary
    // requirements file should contain names of the local packages relative to the dependencies
    // volume and local packages should have been copied to the dependencies volume.
    Assert.assertEquals(3, expectedUpdatedRequirementsFileLines.size());
    Assert.assertTrue(expectedUpdatedRequirementsFileLines.contains(dependency1FileName));
    Assert.assertTrue(expectedUpdatedRequirementsFileLines.contains(dependency2FileName));
    Assert.assertTrue(expectedUpdatedRequirementsFileLines.contains("pypipackage"));

    Assert.assertTrue(
        Paths.get(expectedTempDir.toString(), "dependencies_dir", dependency1FileName)
            .toFile()
            .exists());
    Assert.assertTrue(
        Paths.get(expectedTempDir.toString(), "dependencies_dir", dependency2FileName)
            .toFile()
            .exists());
  }
}
