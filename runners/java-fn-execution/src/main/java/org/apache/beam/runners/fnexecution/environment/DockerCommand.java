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
package org.apache.beam.runners.fnexecution.environment;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A docker command wrapper. Simplifies communications with the Docker daemon. */
// Suppressing here due to https://github.com/spotbugs/spotbugs/issues/724
@SuppressFBWarnings(
    value = "OS_OPEN_STREAM",
    justification = "BufferedReader wraps stream we don't own and should not close")
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class DockerCommand {
  private static final Logger LOG = LoggerFactory.getLogger(DockerCommand.class);

  private static final String DEFAULT_DOCKER_COMMAND = "docker";
  // TODO: Should we require 64-character container ids? Docker technically allows abbreviated ids,
  // but we _should_ always capture full ids.
  private static final Pattern CONTAINER_ID_PATTERN = Pattern.compile("\\p{XDigit}{64}");

  /**
   * Return a DockerCommand instance with default timeout settings: pull timeout 10 min and other
   * command timeout 2 min.
   */
  public static DockerCommand getDefault() {
    return forExecutable(DEFAULT_DOCKER_COMMAND, Duration.ofMinutes(2), Duration.ofMinutes(10));
  }

  static DockerCommand forExecutable(
      String dockerExecutable, Duration commandTimeout, Duration pullTimeout) {
    return new DockerCommand(dockerExecutable, commandTimeout, pullTimeout);
  }

  private final String dockerExecutable;
  private final Duration commandTimeout;
  // pull remote image can take longer time
  private final Duration pullTimeout;

  private DockerCommand(String dockerExecutable, Duration commandTimeout, Duration pullTimeout) {
    this.dockerExecutable = dockerExecutable;
    this.commandTimeout = commandTimeout;
    this.pullTimeout = pullTimeout;
  }

  /**
   * Runs the given container image with the given command line arguments. Returns the running
   * container id.
   *
   * @param imageTag the name of the image to run
   * @param dockerOpts options to provide to docker
   * @param args arguments to provide to the container
   */
  public String runImage(String imageTag, List<String> dockerOpts, List<String> args)
      throws IOException, TimeoutException, InterruptedException {
    checkArgument(!imageTag.isEmpty(), "Docker image tag required");
    // Pull the image from docker repo. This will be no-op if the image already exists.
    try {
      runShortCommand(
          ImmutableList.<String>builder().add(dockerExecutable).add("pull").add(imageTag).build(),
          pullTimeout);
    } catch (IOException | TimeoutException | InterruptedException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unable to pull docker image {}", imageTag, e);
      } else {
        LOG.warn("Unable to pull docker image {}, cause: {}", imageTag, e.getMessage());
      }
    }
    // TODO: Validate args?
    return runShortCommand(
        ImmutableList.<String>builder()
            .add(dockerExecutable)
            .add("run")
            .add("-d")
            .addAll(dockerOpts)
            .add(imageTag)
            .addAll(args)
            .build());
  }

  /**
   * Check if the given container state is running.
   *
   * @param containerId Id of the container to check
   */
  public boolean isContainerRunning(String containerId)
      throws IOException, TimeoutException, InterruptedException {
    checkArgument(!containerId.isEmpty(), "Docker containerId required");
    // TODO: Validate args?
    return runShortCommand(
            ImmutableList.<String>builder()
                .add(dockerExecutable)
                .add("inspect")
                .add("-f")
                .add("{{.State.Running}}")
                .add(containerId)
                .build())
        .equalsIgnoreCase("true");
  }

  /**
   * Returns logs for the container specified by {@code containerId}.
   *
   * @throws IOException if an IOException occurs or if the given container id does not exist
   */
  public String getContainerLogs(String containerId)
      throws IOException, TimeoutException, InterruptedException {
    checkArgument(containerId != null);
    checkArgument(
        CONTAINER_ID_PATTERN.matcher(containerId).matches(),
        "Container ID must be a 64-character hexadecimal string");
    return runShortCommand(
        Arrays.asList(dockerExecutable, "logs", containerId), true, "\n", commandTimeout);
  }

  /**
   * Kills a docker container by container id.
   *
   * @throws IOException if an IOException occurs or if the given container id does not exist
   */
  public void killContainer(String containerId)
      throws IOException, TimeoutException, InterruptedException {
    checkArgument(containerId != null);
    checkArgument(
        CONTAINER_ID_PATTERN.matcher(containerId).matches(),
        "Container ID must be a 64-character hexadecimal string");
    runShortCommand(Arrays.asList(dockerExecutable, "kill", containerId));
  }

  /**
   * Removes docker container with container id.
   *
   * @throws IOException if an IOException occurs, or if the given container id either does not
   *     exist or is still running
   */
  public void removeContainer(String containerId)
      throws IOException, TimeoutException, InterruptedException {
    checkArgument(containerId != null);
    checkArgument(
        CONTAINER_ID_PATTERN.matcher(containerId).matches(),
        "Container ID must be a 64-character hexadecimal string");
    runShortCommand(Arrays.asList(dockerExecutable, "rm", containerId));
  }

  private String runShortCommand(List<String> invocation)
      throws IOException, TimeoutException, InterruptedException {
    return runShortCommand(invocation, false, "", commandTimeout);
  }

  private String runShortCommand(List<String> invocation, Duration timeout)
      throws IOException, TimeoutException, InterruptedException {
    return runShortCommand(invocation, false, "", timeout);
  }

  /**
   * Runs a command, blocks until {@link DockerCommand#commandTimeout} has elapsed, then returns the
   * command's output.
   *
   * @param invocation command and arguments to be run
   * @param redirectErrorStream if true, include the process's stderr in the return value
   * @param delimiter used for separating output lines
   * @return stdout of the command, including stderr if {@code redirectErrorStream} is true
   * @throws TimeoutException if command has not finished by {@link DockerCommand#commandTimeout}
   */
  private String runShortCommand(
      List<String> invocation,
      boolean redirectErrorStream,
      CharSequence delimiter,
      Duration timeout)
      throws IOException, TimeoutException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(invocation);
    pb.redirectErrorStream(redirectErrorStream);
    Process process = pb.start();
    // TODO: Consider supplying executor service here.
    CompletableFuture<String> resultString =
        CompletableFuture.supplyAsync(
            () -> {
              // NOTE: We do not own the underlying stream and do not close it.
              BufferedReader reader =
                  new BufferedReader(
                      new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
              return reader.lines().collect(Collectors.joining(delimiter));
            });
    CompletableFuture<String> errorFuture;
    String errorStringName;
    if (redirectErrorStream) {
      // The standard output and standard error are combined into one stream.
      errorStringName = "stdout and stderr";
      errorFuture = resultString;
    } else {
      // The error stream is separate, and we only consume it in the case of an error.
      errorStringName = "stderr";
      errorFuture =
          CompletableFuture.supplyAsync(
              () -> {
                BufferedReader reader =
                    new BufferedReader(
                        new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8));
                return reader.lines().collect(Collectors.joining(delimiter));
              });
    }
    // TODO: Retry on interrupt?
    boolean processDone = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
    if (!processDone) {
      process.destroy();
      throw new TimeoutException(
          String.format(
              "Timed out while waiting for command '%s'",
              invocation.stream().collect(Collectors.joining(" "))));
    }
    int exitCode = process.exitValue();
    if (exitCode != 0) {
      String errorString;
      try {
        errorString = errorFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (Exception stderrEx) {
        errorString =
            String.format("Error capturing %s: %s", errorStringName, stderrEx.getMessage());
      }
      throw new IOException(
          String.format(
              "Received exit code %d for command '%s'. %s: %s",
              exitCode,
              invocation.stream().collect(Collectors.joining(" ")),
              errorStringName,
              errorString));
    }
    try {
      return resultString.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // Recast any exceptions in reading output as IOExceptions.
      throw new IOException(cause);
    }
  }
}
