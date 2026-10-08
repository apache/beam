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
package org.apache.beam.examples.subprocess.utils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.beam.examples.subprocess.configuration.SubProcessConfiguration;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileUtilsTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void copyFileFromGCSToWorkerAtomicallyReplacesExecutable() throws Exception {
    assumeTrue(SystemUtils.IS_OS_LINUX);

    File sourceDirectory = temporaryFolder.newFolder("source");
    File workerDirectory = temporaryFolder.newFolder("worker");
    String fileName = "echo.sh";
    Path source = sourceDirectory.toPath().resolve(fileName);
    Path destination = workerDirectory.toPath().resolve(fileName);
    Files.write(source, "#!/bin/sh\nexit 0\n".getBytes(UTF_8));
    Files.write(destination, "#!/bin/sh\nexit 1\n".getBytes(UTF_8));
    assertTrue(destination.toFile().setExecutable(true));
    Set<PosixFilePermission> destinationPermissions = Files.getPosixFilePermissions(destination);

    SubProcessConfiguration configuration = new SubProcessConfiguration();
    configuration.setSourcePath(sourceDirectory.getAbsolutePath());
    configuration.setWorkerPath(workerDirectory.getAbsolutePath());

    try (FileChannel ignored = FileChannel.open(destination, WRITE)) {
      String copiedFile =
          FileUtils.copyFileFromGCSToWorker(new ExecutableFile(configuration, fileName));

      assertEquals(destination.toString(), copiedFile);
      assertArrayEquals(Files.readAllBytes(source), Files.readAllBytes(destination));
      assertEquals(destinationPermissions, Files.getPosixFilePermissions(destination));
      assertTrue(Files.isExecutable(destination));
      try (Stream<Path> files = Files.list(workerDirectory.toPath())) {
        assertEquals(1, files.count());
      }

      Process process = new ProcessBuilder(destination.toString()).start();
      assertEquals(0, process.waitFor());
    }
  }
}
