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

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.beam.examples.subprocess.configuration.SubProcessConfiguration;
import org.apache.beam.examples.subprocess.utils.CallingSubProcessUtils;
import org.apache.beam.examples.subprocess.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the process kernel which deals with exec of the subprocess. It also deals with all I/O.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SubProcessKernel {

  private static final Logger LOG = LoggerFactory.getLogger(SubProcessKernel.class);

  private static final int MAX_SIZE_COMMAND_LINE_ARGS = 128 * 1024;

  SubProcessConfiguration configuration;
  ProcessBuilder processBuilder;

  private SubProcessKernel() {}

  /**
   * Creates the SubProcess Kernel ready for execution. Will deal with all input and outputs to the
   * SubProcess
   *
   * @param options
   * @param binaryName
   */
  public SubProcessKernel(SubProcessConfiguration options, String binaryName) {
    this.configuration = options;
    this.processBuilder = new ProcessBuilder(binaryName);
  }

  public List<String> exec(SubProcessCommandLineArgs commands) throws Exception {
    try (CallingSubProcessUtils.Permit permit =
        new CallingSubProcessUtils.Permit(processBuilder.command().get(0))) {

      List<String> results = null;

      try (SubProcessIOFiles outputFiles = new SubProcessIOFiles(configuration.getWorkerPath())) {

        try {
          Process process = execBinary(processBuilder, commands, outputFiles);
          results = collectProcessResults(process, processBuilder, outputFiles);
        } catch (Exception ex) {
          LOG.error("Error running executable ", ex);
          throw ex;
        }
      } catch (IOException ex) {
        LOG.error(
            "Unable to delete the outputfiles. This can lead to performance issues and failure",
            ex);
      }
      return results;
    }
  }

  public byte[] execBinaryResult(SubProcessCommandLineArgs commands) throws Exception {
    try (CallingSubProcessUtils.Permit permit =
        new CallingSubProcessUtils.Permit(processBuilder.command().get(0))) {

      try (SubProcessIOFiles outputFiles = new SubProcessIOFiles(configuration.getWorkerPath())) {

        try {
          Process process = execBinary(processBuilder, commands, outputFiles);
          return collectProcessResultsBytes(process, processBuilder, outputFiles);
        } catch (Exception ex) {
          LOG.error("Error running executable ", ex);
          throw ex;
        }
      } catch (IOException ex) {
        LOG.error(
            "Unable to delete the outputfiles. This can lead to performance issues and failure",
            ex);
      }
      return new byte[0];
    }
  }

  private ProcessBuilder prepareBuilder(
      ProcessBuilder builder, SubProcessCommandLineArgs commands, SubProcessIOFiles outPutFiles)
      throws IllegalStateException {

    // Check we are not over the max size of command line parameters
    if (getTotalCommandBytes(commands) > MAX_SIZE_COMMAND_LINE_ARGS) {
      throw new IllegalStateException("Command is over 2MB in size");
    }

    appendExecutablePath(builder);

    // Add the result file path to the builder at position 1, 0 is reserved for the process itself
    builder.command().add(1, outPutFiles.resultFile.toString());

    // Shift commands by 2 ordinal positions and load into the builder
    for (SubProcessCommandLineArgs.Command s : commands.getParameters()) {
      builder.command().add(s.ordinalPosition + 2, s.value);
    }

    builder.redirectError(Redirect.appendTo(outPutFiles.errFile.toFile()));
    builder.redirectOutput(Redirect.appendTo(outPutFiles.outFile.toFile()));

    return builder;
  }

  /**
   * Add up the total bytes used by the process.
   *
   * @param commands
   * @return
   */
  private int getTotalCommandBytes(SubProcessCommandLineArgs commands) {
    int size = 0;
    for (SubProcessCommandLineArgs.Command c : commands.getParameters()) {
      size += c.value.length();
    }
    return size;
  }

  private Process execBinary(
      ProcessBuilder builder, SubProcessCommandLineArgs commands, SubProcessIOFiles outPutFiles)
      throws Exception {
    try {

      builder = prepareBuilder(builder, commands, outPutFiles);
      Process process = builder.start();

      boolean timeout = !process.waitFor(configuration.getWaitTime(), TimeUnit.SECONDS);

      if (timeout) {
        String log =
            String.format(
                "Timeout waiting to run process with parameters %s . "
                    + "Check to see if your timeout is long enough. Currently set at %s.",
                createLogEntryFromInputs(builder.command()), configuration.getWaitTime());
        throw new Exception(log);
      }
      return process;

    } catch (Exception ex) {

      LOG.error(
          String.format(
              "Error running process with parameters %s error was %s ",
              createLogEntryFromInputs(builder.command()), ex.getMessage()));
      throw new Exception(ex);
    }
  }

  /**
   * TODO clean up duplicate with byte[] version collectBinaryProcessResults.
   *
   * @param process
   * @param builder
   * @param outPutFiles
   * @return List of results
   * @throws Exception if process has non 0 value or no logs found then throw exception
   */
  private List<String> collectProcessResults(
      Process process, ProcessBuilder builder, SubProcessIOFiles outPutFiles) throws Exception {

    List<String> results = new ArrayList<>();

    try {

      LOG.debug(String.format("Executing process %s", createLogEntryFromInputs(builder.command())));

      // If process exit value is not 0 then subprocess failed, record logs
      if (process.exitValue() != 0) {
        outPutFiles.copyOutPutFilesToBucket(configuration, FileUtils.toStringParams(builder));
        String log = createLogEntryForProcessFailure(process, builder.command(), outPutFiles);
        throw new Exception(log);
      }

      // If no return file then either something went wrong or the binary is setup incorrectly for
      // the ret file either way throw error
      if (!Files.exists(outPutFiles.resultFile)) {
        String log = createLogEntryForProcessFailure(process, builder.command(), outPutFiles);
        outPutFiles.copyOutPutFilesToBucket(configuration, FileUtils.toStringParams(builder));
        throw new Exception(log);
      }

      // Everything looks healthy return values
      try (Stream<String> lines = Files.lines(outPutFiles.resultFile)) {
        for (String line : (Iterable<String>) lines::iterator) {
          results.add(line);
        }
      }
      return results;
    } catch (Exception ex) {
      String log =
          String.format(
              "Unexpected error runnng process. %s error message was %s",
              createLogEntryFromInputs(builder.command()), ex.getMessage());
      throw new Exception(log);
    }
  }

  /**
   * Used when the reault file contains binary data.
   *
   * @param process
   * @param builder
   * @param outPutFiles
   * @return Binary results
   * @throws Exception if process has non 0 value or no logs found then throw exception
   */
  private byte[] collectProcessResultsBytes(
      Process process, ProcessBuilder builder, SubProcessIOFiles outPutFiles) throws Exception {

    Byte[] results;

    try {

      LOG.debug(String.format("Executing process %s", createLogEntryFromInputs(builder.command())));

      // If process exit value is not 0 then subprocess failed, record logs
      if (process.exitValue() != 0) {
        outPutFiles.copyOutPutFilesToBucket(configuration, FileUtils.toStringParams(builder));
        String log = createLogEntryForProcessFailure(process, builder.command(), outPutFiles);
        throw new Exception(log);
      }

      // If no return file then either something went wrong or the binary is setup incorrectly for
      // the ret file either way throw error
      if (!Files.exists(outPutFiles.resultFile)) {
        String log = createLogEntryForProcessFailure(process, builder.command(), outPutFiles);
        outPutFiles.copyOutPutFilesToBucket(configuration, FileUtils.toStringParams(builder));
        throw new Exception(log);
      }

      // Everything looks healthy return bytes
      return Files.readAllBytes(outPutFiles.resultFile);

    } catch (Exception ex) {
      String log =
          String.format(
              "Unexpected error runnng process. %s error message was %s",
              createLogEntryFromInputs(builder.command()), ex.getMessage());
      throw new Exception(log);
    }
  }

  private static String createLogEntryForProcessFailure(
      Process process, List<String> commands, SubProcessIOFiles files) {

    StringBuilder stringBuilder = new StringBuilder();

    // Highlight when no result file is found vs standard process error
    if (process.exitValue() == 0) {
      stringBuilder.append(String.format("%nProcess succeded but no result file was found %n"));
    } else {
      stringBuilder.append(
          String.format("%nProcess error failed with exit value of %s %n", process.exitValue()));
    }

    stringBuilder.append(
        String.format("Command info was %s %n", createLogEntryFromInputs(commands)));

    stringBuilder.append(
        String.format(
            "First line of error file is  %s %n", FileUtils.readLineOfLogFile(files.errFile)));

    stringBuilder.append(
        String.format(
            "First line of out file is %s %n", FileUtils.readLineOfLogFile(files.outFile)));

    stringBuilder.append(
        String.format(
            "First line of ret file is %s %n", FileUtils.readLineOfLogFile(files.resultFile)));

    return stringBuilder.toString();
  }

  private static String createLogEntryFromInputs(List<String> commands) {
    String params;
    if (commands != null) {
      params = String.join(",", commands);
    } else {
      params = "No-Commands";
    }
    return params;
  }

  // Pass the Path of the binary to the SubProcess in Command position 0
  private ProcessBuilder appendExecutablePath(ProcessBuilder builder) {
    String executable = builder.command().get(0);
    if (executable == null) {
      throw new IllegalArgumentException(
          "No executable provided to the Process Builder... we will do... nothing... ");
    }
    builder
        .command()
        .set(0, FileUtils.getFileResourceId(configuration.getWorkerPath(), executable).toString());
    return builder;
  }
}
