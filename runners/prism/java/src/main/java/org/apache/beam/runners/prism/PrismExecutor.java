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
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
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

  private static final Logger LOG = LoggerFactory.getLogger(PrismExecutor.class);
  static final String IDLE_SHUTDOWN_TIMEOUT = "-idle_shutdown_timeout=%s";
  static final String JOB_PORT_FLAG_TEMPLATE = "-job_port=%s";
  static final String SERVE_HTTP_FLAG_TEMPLATE = "-serve_http=%s";

  protected @MonotonicNonNull Process process;
  protected ExecutorService executorService = Executors.newSingleThreadExecutor();
  protected @MonotonicNonNull Future<?> future = null;

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

  /** Stops the execution of the {@link Process}, created as a result of {@link #execute}. */
  void stop() {
    LOG.info("Stopping Prism...");
    if (future != null) {
      future.cancel(true);
    }
    executorService.shutdown();
    try {
      boolean ignored = executorService.awaitTermination(5000L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {
    }
    if (process == null) {
      return;
    }
    if (!process.isAlive()) {
      return;
    }
    process.destroy();
    try {
      process.waitFor();
    } catch (InterruptedException ignored) {
    }
  }

  /** Reports whether the Prism executable {@link Process#isAlive()}. */
  boolean isAlive() {
    if (process == null) {
      return false;
    }
    return process.isAlive();
  }

  /**
   * Execute the {@link ProcessBuilder} that starts the Prism service. Redirects output to STDOUT.
   */
  void execute() throws IOException {
    execute(createProcessBuilder().inheritIO());
  }

  /**
   * Execute the {@link ProcessBuilder} that starts the Prism service. Redirects output to the
   * {@param outputStream}.
   */
  void execute(OutputStream outputStream) throws IOException {
    execute(createProcessBuilder().redirectErrorStream(true));
    this.future =
        executorService.submit(
            () -> {
              try {
                ByteStreams.copy(checkStateNotNull(process).getInputStream(), outputStream);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }

  /**
   * Execute the {@link ProcessBuilder} that starts the Prism service. Redirects output to the
   * {@param file}.
   */
  void execute(File file) throws IOException {
    execute(
        createProcessBuilder()
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.appendTo(file)));
  }

  private void execute(ProcessBuilder processBuilder) throws IOException {
    this.process = processBuilder.start();
    LOG.info("started {}", String.join(" ", getCommandWithArguments()));
  }

  private List<String> getCommandWithArguments() {
    List<String> commandWithArguments = new ArrayList<>();
    commandWithArguments.add(getCommand());
    commandWithArguments.addAll(getArguments());

    return commandWithArguments;
  }

  private ProcessBuilder createProcessBuilder() {
    return new ProcessBuilder(getCommandWithArguments());
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setCommand(String command);

    abstract Builder setArguments(List<String> arguments);

    abstract Optional<List<String>> getArguments();

    abstract PrismExecutor autoBuild();

    final PrismExecutor build() {
      if (!getArguments().isPresent()) {
        setArguments(Collections.emptyList());
      }
      return autoBuild();
    }
  }
}
