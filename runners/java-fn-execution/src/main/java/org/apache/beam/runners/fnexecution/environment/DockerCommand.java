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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

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
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A docker command wrapper. Simplifies communications with the Docker daemon. */
// Suppressing here due to https://github.com/spotbugs/spotbugs/issues/724
@SuppressFBWarnings(
    value = "OS_OPEN_STREAM",
    justification = "BufferedReader wraps stream we don't own and should not close")
class DockerCommand {
  private static final Logger LOG = LoggerFactory.getLogger(DockerCommand.class);

  private static final String DEFAULT_DOCKER_COMMAND = "docker";
  // TODO: Should we require 64-character container ids? Docker technically allows abbreviated ids,
  // but we _should_ always capture full ids.
  private static final Pattern CONTAINER_ID_PATTERN = Pattern.compile("\\p{XDigit}{64}");

  public static DockerCommand getDefault() {
    return forExecutable(DEFAULT_DOCKER_COMMAND, Duration.ofMinutes(2));
  }

  static DockerCommand forExecutable(String dockerExecutable, Duration commandTimeout) {
    return new DockerCommand(dockerExecutable, commandTimeout);
  }

  private final String dockerExecutable;
  private final Duration commandTimeout;

  private DockerCommand(String dockerExecutable, Duration commandTimeout) {
    this.dockerExecutable = dockerExecutable;
    this.commandTimeout = commandTimeout;
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
          ImmutableList.<String>builder().add(dockerExecutable).add("pull").add(imageTag).build());
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

  /** Run the given command invocation and return stdout as a String. */
  private String runShortCommand(List<String> invocation)
      throws IOException, TimeoutException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(invocation);
    Process process = pb.start();
    // TODO: Consider supplying executor service here.
    CompletableFuture<String> resultString =
        CompletableFuture.supplyAsync(
            () -> {
              // NOTE: We do not own the underlying stream and do not close it.
              BufferedReader reader =
                  new BufferedReader(
                      new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
              return reader.lines().collect(Collectors.joining());
            });
    // NOTE: We only consume the error string in the case of an error.
    CompletableFuture<String> errorFuture =
        CompletableFuture.supplyAsync(
            () -> {
              BufferedReader reader =
                  new BufferedReader(
                      new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8));
              return reader.lines().collect(Collectors.joining());
            });
    // TODO: Retry on interrupt?
    boolean processDone = process.waitFor(commandTimeout.toMillis(), TimeUnit.MILLISECONDS);
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
        errorString = errorFuture.get(commandTimeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (Exception stderrEx) {
        errorString = String.format("Error capturing stderr: %s", stderrEx.getMessage());
      }
      throw new IOException(
          String.format(
              "Received exit code %d for command '%s'. stderr: %s",
              exitCode, invocation.stream().collect(Collectors.joining(" ")), errorString));
    }
    try {
      // TODO: Consider a stricter timeout.
      return resultString.get(commandTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // Recast any exceptions in reading output as IOExceptions.
      throw new IOException(cause);
    }
  }
}
