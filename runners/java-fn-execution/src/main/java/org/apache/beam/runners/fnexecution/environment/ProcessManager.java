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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple process manager which forks processes and kills them if necessary. */
@ThreadSafe
class ProcessManager {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessManager.class);

  private static final ProcessManager INSTANCE = new ProcessManager();

  private final Map<String, Process> processes;

  public static ProcessManager getInstance() {
    return INSTANCE;
  }

  private ProcessManager() {
    this.processes = Collections.synchronizedMap(new HashMap<>());
  }

  static class RunningProcess {
    private Process process;

    RunningProcess(Process process) {
      this.process = process;
    }

    /** Checks if the underlying process is still running. */
    void isAliveOrThrow() throws IllegalStateException {
      if (!process.isAlive()) {
        throw new IllegalStateException("Process died with exit code " + process.exitValue());
      }
    }
  }

  /**
   * Forks a process with the given command and arguments.
   *
   * @param id A unique id for the process
   * @param command the name of the executable to run
   * @param args arguments to provide to the executable
   * @return A RunningProcess which can be checked for liveness
   */
  RunningProcess startProcess(String id, String command, List<String> args) throws IOException {
    return startProcess(id, command, args, Collections.emptyMap());
  }

  /**
   * Forks a process with the given command, arguments, and additional environment variables.
   *
   * @param id A unique id for the process
   * @param command The name of the executable to run
   * @param args Arguments to provide to the executable
   * @param env Additional environment variables for the process to be forked
   * @return A RunningProcess which can be checked for liveness
   */
  RunningProcess startProcess(String id, String command, List<String> args, Map<String, String> env)
      throws IOException {
    checkNotNull(id, "Process id must not be null");
    checkNotNull(command, "Command must not be null");
    checkNotNull(args, "Process args must not be null");
    checkNotNull(env, "Environment map must not be null");

    ProcessBuilder pb =
        new ProcessBuilder(ImmutableList.<String>builder().add(command).addAll(args).build());
    pb.environment().putAll(env);

    LOG.debug("Attempting to start process with command: " + pb.command());
    Process newProcess = pb.start();
    // Pipe stdout and stderr to /dev/null to avoid blocking the process due to filled PIPE buffer
    pb.redirectErrorStream(true);
    if (System.getProperty("os.name", "").startsWith("Windows")) {
      pb.redirectOutput(new File("nul"));
    } else {
      pb.redirectOutput(new File("/dev/null"));
    }

    Process oldProcess = processes.put(id, newProcess);
    if (oldProcess != null) {
      oldProcess.destroy();
      newProcess.destroy();
      throw new IllegalStateException("There was already a process running with id " + id);
    }

    return new RunningProcess(newProcess);
  }

  /** Stops a previously started process identified by its unique id. */
  void stopProcess(String id) {
    checkNotNull(id, "Process id must not be null");
    Process process = checkNotNull(processes.remove(id), "Process for id does not exist: " + id);
    LOG.debug("Attempting to stop process with id {}", id);
    if (process.isAlive()) {
      // first try to kill gracefully
      process.destroy();
      long maxTimeToWait = 2000;
      if (waitForProcessToDie(process, maxTimeToWait)) {
        LOG.info("Process for worker {} still running. Killing.", id);
        process.destroyForcibly();
      }
      if (waitForProcessToDie(process, maxTimeToWait)) {
        LOG.warn("Process for worker {} could not be killed.", id);
      }
    }
  }

  private static boolean waitForProcessToDie(Process process, long maxWaitTimeMillis) {
    final long startTime = System.currentTimeMillis();
    boolean processIsAlive;
    do {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting on process", e);
      }
      processIsAlive = process.isAlive();
    } while (processIsAlive && System.currentTimeMillis() - startTime < maxWaitTimeMillis);
    return processIsAlive;
  }
}
