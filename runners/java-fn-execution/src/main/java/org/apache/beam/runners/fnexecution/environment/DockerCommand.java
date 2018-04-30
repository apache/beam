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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
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

/** A docker command wrapper. Simplifies communications with the Docker daemon. */
class DockerCommand {
  // TODO: Should we require 64-character container ids? Docker technically allows abbreviated ids,
  // but we _should_ always capture full ids.
  private static final Pattern CONTAINER_ID_PATTERN = Pattern.compile("\\p{XDigit}{64}");

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
   */
  public String runImage(String imageTag, List<String> args)
      throws IOException, TimeoutException, InterruptedException {
    checkArgument(!imageTag.isEmpty(), "Docker image tag required");
    // TODO: Validate args?
    return runShortCommand(
        ImmutableList.<String>builder()
            .add(dockerExecutable)
            .add("run")
            .add("-d")
            .add(imageTag)
            .addAll(args)
            .build());
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
