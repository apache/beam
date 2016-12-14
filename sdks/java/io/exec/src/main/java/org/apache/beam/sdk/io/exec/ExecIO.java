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
package org.apache.beam.sdk.io.exec;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.Serializable;

import javax.annotation.Nullable;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.ShutdownHookProcessDestroyer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to execute system commands.
 *
 * <h3>ExecFn to execute a command</h3>
 *
 * <p>This IO provides {@link ExecFn} function. This function is able to execute a system command.
 *
 * <p>The {@link ExecFn} function expects a {@code PCollection<String>} containing the system
 * commands to execute.
 *
 * <p>Then the {@link ExecFn} function executes the commands and populate a {@code
 * PCollection<String>} containing the command execution output.
 *
 * <p>You can use the {@link ExecFn} function as a step in your pipeline using {@link ParDo}.
 *
 * <p>The following example illustrate this:
 *
 * <pre>{@code
 *   PCollection<String> commands = pipeline.apply(...);
 *
 *   PCollection<String> commandsOutput = commands.apply(ParDo.of(new ExecIO.ExecFn()));
 * }</pre>
 *
 * <p>The {@link ExecFn} function also accepts a {@link ExecOptions} allowing you to define the
 * execution working directory and execution timeout.
 *
 * <p>The following example illustrate how to use {@link ExecOptions} in {@link ExecFn}:
 *
 * <pre>{@code
 *   PCollection<String> commands = pipeline.apply(...);
 *
 *   PCollection<String> commandsOutput = commands
 *      .apply(ParDo.of(
 *        new ExecIO.ExecFn(ExecIO.ExecOptions("/path/to/working/directory", 10000)))
 *      );
 *
 * }</pre>
 *
 * <p>For convenience, {@link ExecIO} also provides {@link Read} and {@link Write}
 * {@link PTransform}s.</p>
 *
 * <h3>Starting a pipeline with a command</h3>
 *
 * <p>The {@link Read} {@link PTransform} executes a given command and starts the pipeline with the
 * command execution output.
 *
 * <p>The following example illustrates how to use {@code ExecIO.read()}:
 *
 * <pre>{@code
 *   PCollection<String> commandOutput = pipeline.apply(ExecIO.read().withCommand("command"));
 * }</pre>
 *
 * <p>{@code ExecIO.read()} also accepts {@code withWorkingDir(workingDir)} and
 * {@code withTimeout(timeout)} methods to define the execution options.
 *
 * <h3>Executing a command at the end of a pipeline</h3>
 *
 * <p>The {@link Write} {@link PTransform} executes the commands define in the input
 * {@link PCollection} and terminate the pipeline.
 *
 * <p>The following example illustrates how to use {@code ExecIO.write()}:
 *
 * <pre>{@code
 *  PCollection<String> commands = pipeline.apply(...);
 *
 *  commands.apply(ExecIO.write());
 * }</pre>
 *
 * <p>Like the {@link Read}, the {@link Write} accepts {@code withWorkingDir(workingDir)} and
 * {@code withTimeout(timeout)} methods to define the execution options.
 */
public class ExecIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecIO.class);

  public static Read read() {
    return new AutoValue_ExecIO_Read.Builder()
        .setExecOptions(ExecOptions.create()).build();
  }

  public static Write write() {
    return new AutoValue_ExecIO_Write.Builder()
        .setExecOptions(ExecOptions.create()).build();
  }

  /**
   * Convenient {@link PTransform} to start a pipeline with a system command execution.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    @Nullable abstract String getCommand();
    @Nullable abstract ExecOptions getExecOptions();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCommand(String command);
      abstract Builder setExecOptions(ExecOptions execOptions);
      abstract Read build();
    }

    public Read withCommand(String command) {
      checkArgument(command != null,
          "ExecIO.read().withCommand(command) called with null command");
      return builder().setCommand(command).build();
    }

    public Read withWorkingDir(String workingDir) {
      checkArgument(workingDir != null,
          "ExecIO.read().withWorkingDir(workingDir) called with null workingDir");
      return builder()
          .setExecOptions(ExecOptions.create(workingDir, getExecOptions().getTimeout()))
          .build();
    }

    public Read withTimeout(long timeout) {
      checkArgument(timeout >= 0,
          "ExecIO.read().withTimeout(timeout) called with invalid timeout");
      return builder()
          .setExecOptions(ExecOptions.create(getExecOptions().getWorkingDir(), timeout))
          .build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input
          .apply(Create.of(getCommand()))
          .apply(ParDo.of(new ExecFn(getExecOptions())));
    }

    @Override
    public void validate(PBegin input) {
      checkState(getCommand() != null,
          "ExecIO.read() requires a command to be set via withCommand(command)");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("command", getCommand()));
      builder.addIfNotNull(DisplayData.item("workingDir", getExecOptions().getWorkingDir()));
      builder.addIfNotNull(DisplayData.item("timeout", getExecOptions().getTimeout()));
    }

  }

  /**
   * Convenient {@link PTransform} to execute commands using the elements in a
   * {@link PCollection} and terminate the pipeline.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {

    abstract ExecOptions getExecOptions();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setExecOptions(ExecOptions execOptions);
      abstract Write build();
    }

    public Write withWorkingDir(String workingDir) {
      checkArgument(workingDir != null,
          "ExecIO.write().withWorkingDir(workingDir) called with null workingDir");
      return builder()
          .setExecOptions(ExecOptions.create(workingDir, getExecOptions().getTimeout()))
          .build();
    }

    public Write withTimeout(long timeout) {
      checkArgument(timeout >= 0,
          "ExecIO.write().withTimeout(timeout) called with invalid timeout");
      return builder()
          .setExecOptions(ExecOptions.create(getExecOptions().getWorkingDir(), timeout))
          .build();
    }

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(ParDo.of(new ExecFn(getExecOptions())));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PCollection<String> input) {
      // nothing to do
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.addIfNotNull(DisplayData.item("workingDir", getExecOptions().getWorkingDir()));
      builder.add(DisplayData.item("timeout", getExecOptions().getTimeout()));
    }

  }

  /**
   * A POJO describing system command execution options.
   */
  @AutoValue
  public abstract static class ExecOptions implements Serializable {

    @Nullable abstract String getWorkingDir();
    abstract long getTimeout();

    public static ExecOptions create() {
      return new AutoValue_ExecIO_ExecOptions(null, Long.MAX_VALUE);
    }

    public static ExecOptions create(String workingDir) {
      return new AutoValue_ExecIO_ExecOptions(workingDir, Long.MAX_VALUE);
    }

    public static ExecOptions create(long timeout) {
      return new AutoValue_ExecIO_ExecOptions(null, timeout);
    }

    public static ExecOptions create(String workingDir, long timeout) {
      return new AutoValue_ExecIO_ExecOptions(workingDir, timeout);
    }

  }

  /**
   * Main {@link DoFn} which actually executes system commands contained in the input
   * {@link PCollection}.
   * The output {@link PCollection} contains the commands execution output.
   * This function optionally accepts {@link ExecOptions} as constructor argument.
   */
  public static class ExecFn extends DoFn<String, String> {


    private ExecOptions options;

    public ExecFn() {
      this.options = ExecOptions.create();
    }

    public ExecFn(ExecOptions options) {
      this.options = options;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      String command = context.element();

      checkState(command != null, "ExecIO executed with null command");

      ByteArrayOutputStream out = new ByteArrayOutputStream();

      DefaultExecutor executor = new DefaultExecutor();
      PumpStreamHandler handler = new PumpStreamHandler(out, out);
      executor.setStreamHandler(handler);
      executor.setWatchdog(new ExecuteWatchdog(options.getTimeout()));
      executor.setProcessDestroyer(new ShutdownHookProcessDestroyer());
      executor.setExitValues(null);
      if (options.getWorkingDir() != null) {
        executor.setWorkingDirectory(new File(options.getWorkingDir()).getAbsoluteFile());
      }

      CommandLine commandLine = CommandLine.parse(command);

      try {
        LOGGER.debug("Executing command: {}", command);
        executor.execute(commandLine);
        context.output(out.toString());
      } catch (Exception e) {
        LOGGER.error("Command {} execution failed: {}", command, out, e);
        throw new IllegalStateException("Command " + command + " execution failed: " + e
            .getMessage());
      }

    }
  }

}
