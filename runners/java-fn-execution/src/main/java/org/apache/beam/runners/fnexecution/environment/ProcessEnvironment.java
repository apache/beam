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

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;

/**
 * Environment for process-based execution. The environment is responsible for stopping the process.
 */
public class ProcessEnvironment implements RemoteEnvironment {

  private final ProcessManager processManager;
  private final RunnerApi.Environment environment;
  private final String workerId;
  private final InstructionRequestHandler instructionHandler;

  private boolean isClosed;

  public static RemoteEnvironment create(
      ProcessManager processManager,
      RunnerApi.Environment environment,
      String workerId,
      InstructionRequestHandler instructionHandler) {
    return new ProcessEnvironment(processManager, environment, workerId, instructionHandler);
  }

  private ProcessEnvironment(
      ProcessManager processManager,
      RunnerApi.Environment environment,
      String workerId,
      InstructionRequestHandler instructionHandler) {

    this.processManager = processManager;
    this.environment = environment;
    this.workerId = workerId;
    this.instructionHandler = instructionHandler;
  }

  @Override
  public RunnerApi.Environment getEnvironment() {
    return environment;
  }

  @Override
  public InstructionRequestHandler getInstructionRequestHandler() {
    return instructionHandler;
  }

  @Override
  public synchronized void close() throws Exception {
    if (isClosed) {
      return;
    }
    Exception exception = null;
    try {
      processManager.stopProcess(workerId);
    } catch (Exception e) {
      exception = e;
    }
    try {
      instructionHandler.close();
    } catch (Exception e) {
      if (exception != null) {
        exception.addSuppressed(e);
      } else {
        exception = e;
      }
    }
    isClosed = true;
    if (exception != null) {
      throw exception;
    }
  }
}
