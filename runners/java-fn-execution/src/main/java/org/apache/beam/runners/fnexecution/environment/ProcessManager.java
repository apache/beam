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

  public static ProcessManager getDefault() {
    return INSTANCE;
  }

  private ProcessManager() {
    this.processes = Collections.synchronizedMap(new HashMap<>());
  }

  /**
   * Forks a process with the given command and arguments.
   *
   * @param id A unique id for the process
   * @param command the name of the executable to run
   * @param args arguments to provide to the executable
   */
  void runCommand(String id, String command, List<String> args) throws IOException {
    checkNotNull(id, "Process id must not be null");
    checkNotNull(command, "Command must not be null");
    checkNotNull(args, "Process args must not be null");

    ProcessBuilder pb =
        new ProcessBuilder(ImmutableList.<String>builder().add(command).addAll(args).build());

    LOG.debug("Attempting to start SDK harness with command: " + pb.command());
    Process newProcess = pb.start();
    Process oldProcess = processes.put(id, newProcess);
    if (oldProcess != null) {
      oldProcess.destroy();
      newProcess.destroy();
      throw new IllegalStateException("There was already a process running with id " + id);
    }
  }

  /** Stops a previously started process identified by its unique id. */
  void stopProcess(String id) {
    checkNotNull(id, "Process id must not be null");
    Process process = checkNotNull(processes.remove(id), "Process for id does not exist: " + id);
    if (process.isAlive()) {
      process.destroy();
    }
  }
}
