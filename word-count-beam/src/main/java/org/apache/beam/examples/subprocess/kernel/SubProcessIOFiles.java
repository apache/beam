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
package org.apache.beam.examples.subprocess.kernel;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.beam.examples.subprocess.configuration.SubProcessConfiguration;
import org.apache.beam.examples.subprocess.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All information generated from the process will be stored in output files. The local working
 * directory is used to generate three files with extension .err for standard error output .out for
 * standard out output .ret for storing the results from the called library. The files will have a
 * uuid created for them based on java.util.UUID
 */
public class SubProcessIOFiles implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(SubProcessIOFiles.class);

  Path errFile;
  Path outFile;
  Path resultFile;
  Path base;

  String errFileLocation = "";
  String outFileLocation = "";
  String uuid;

  public String getErrFileLocation() {
    return errFileLocation;
  }

  public String getOutFileLocation() {
    return outFileLocation;
  }

  /** @param workerWorkingDirectory */
  public SubProcessIOFiles(String workerWorkingDirectory) {

    this.uuid = UUID.randomUUID().toString();
    base = Paths.get(workerWorkingDirectory);

    // Setup all the redirect handles, including the return file type
    errFile = Paths.get(base.toString(), uuid + ".err");
    outFile = Paths.get(base.toString(), uuid + ".out");
    resultFile = Paths.get(base.toString(), uuid + ".res");
  }

  public Path getErrFile() {
    return errFile;
  }

  public Path getOutFile() {
    return outFile;
  }

  public Path getResultFile() {
    return resultFile;
  }

  /**
   * Clean up the files that have been created on the local worker file system. Without this expect
   * both performance issues and eventual failure
   */
  @Override
  public void close() throws IOException {

    if (Files.exists(outFile)) {
      Files.delete(outFile);
    }

    if (Files.exists(errFile)) {
      Files.delete(errFile);
    }

    if (Files.exists(resultFile)) {
      Files.delete(resultFile);
    }
  }

  /**
   * Will copy the output files to the GCS path setup via the configuration.
   *
   * @param configuration
   * @param params
   */
  public void copyOutPutFilesToBucket(SubProcessConfiguration configuration, String params) {
    if (Files.exists(outFile) || Files.exists(errFile)) {
      try {
        outFileLocation = FileUtils.copyFileFromWorkerToGCS(configuration, outFile);
      } catch (Exception ex) {
        LOG.error("Error uploading log file to storage ", ex);
      }

      try {
        errFileLocation = FileUtils.copyFileFromWorkerToGCS(configuration, errFile);
      } catch (Exception ex) {
        LOG.error("Error uploading log file to storage ", ex);
      }

      LOG.info(
          String.format(
              "Log Files for process: %s outFile was: %s errFile was: %s",
              params, outFileLocation, errFileLocation));
    } else {
      LOG.error(String.format("There was no output file or err file for process %s", params));
    }
  }
}
