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
package org.apache.beam.runners.prism;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import com.google.errorprone.annotations.FormatMethod;
import com.zaxxer.nuprocess.NuAbstractProcessHandler;
import com.zaxxer.nuprocess.NuProcess;
import com.zaxxer.nuprocess.NuProcessBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PrismExecutor} builds and executes a {@link ProcessBuilder} for use by the {@link
 * PrismRunner}. Prism is a {@link org.apache.beam.runners.portability.PortableRunner} maintained at
 * <a href="https://github.com/apache/beam/tree/master/sdks/go/cmd/prism">sdks/go/cmd/prism</a>.
 */
@AutoValue
abstract class PrismExecutor {

  private final ListeningExecutorService executorService =
      MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
  private final ProcessHandler processHandler = new ProcessHandler();
  private Optional<NuProcess> processOpt = Optional.empty();

  static Builder builder() {
    return new AutoValue_PrismExecutor.Builder();
  }

  /** The command to execute the Prism binary. */
  abstract String getCommand();

  /**
   * Additional arguments to pass when invoking the Prism binary. Defaults to an {@link
   * Collections#emptyList()}.
   */
  abstract List<String> getArguments();

  abstract OutputStream getOutputStream();

  /** Stops the execution of the {@link Process}, created as a result of {@link #execute}. */
  void stop() {
    stopProcess();
    executorService.shutdown();
    try {
      boolean ignored = executorService.awaitTermination(1000L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {
    }
  }

  private void stopProcess() {
    if (!processOpt.isPresent()) {
      return;
    }
    NuProcess process = processOpt.get();
    write("stopping Prism process PID: %s", process.getPID());
    process.destroy(false);
    try {
      process.waitFor(3000L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      process.destroy(true);
    }
  }

  /**
   * Execute the {@link ProcessBuilder} that starts the Prism service. Redirects output to STDOUT.
   */
  void execute() {
    execute(createProcessBuilder());
  }

  private void execute(NuProcessBuilder processBuilder) {
    NuProcess process = processBuilder.start();
    process.wantWrite();
    processOpt = Optional.of(process);
    write(
        "started %s, process PID: %s",
        String.join(" ", getCommandWithArguments()),
        process.getPID());
  }

  private class ProcessHandler extends NuAbstractProcessHandler {

    private @MonotonicNonNull NuProcess process;

    @Override
    public void onPreStart(NuProcess process) {
      this.process = process;
    }

    @Override
    public void onStderr(ByteBuffer buffer, boolean closed) {
      out(buffer, closed);
    }

    @Override
    public void onStdout(ByteBuffer buffer, boolean closed) {
      out(buffer, closed);
    }

    private void out(ByteBuffer buffer, boolean closed) {
      if (closed) {
        return;
      }
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      String message = new String(bytes, StandardCharsets.UTF_8);
      write(message);
    }

    @Override
    public void onExit(int statusCode) {
      NuProcess process = checkStateNotNull(this.process);
      write("exit called with status code %s for process PID: %s", statusCode, process.getPID());
    }
  }

  private List<String> getCommandWithArguments() {
    List<String> commandWithArguments = new ArrayList<>();
    commandWithArguments.add(getCommand());
    commandWithArguments.addAll(getArguments());

    return commandWithArguments;
  }

  private NuProcessBuilder createProcessBuilder() {
    NuProcessBuilder result = new NuProcessBuilder(getCommandWithArguments());
    result.setProcessListener(processHandler);
    return result;
  }

  @FormatMethod
  private void write(String template, Object... args) {
    String message = String.format(template, args);
    write(message + "\n");
  }

  private void write(String message) {
    try {
      getOutputStream().write(message.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setCommand(String command);

    abstract Builder setArguments(List<String> arguments);

    abstract Optional<List<String>> getArguments();

    abstract PrismExecutor autoBuild();

    abstract Builder setOutputStream(OutputStream outputStream);

    final PrismExecutor build() {
      if (!getArguments().isPresent()) {
        setArguments(Collections.emptyList());
      }
      return autoBuild();
    }
  }
}
