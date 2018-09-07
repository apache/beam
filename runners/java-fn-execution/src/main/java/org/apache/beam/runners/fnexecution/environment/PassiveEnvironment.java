package org.apache.beam.runners.fnexecution.environment;

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

import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;

@ThreadSafe
class PassiveEnvironment implements RemoteEnvironment {

  static PassiveEnvironment create(
      RunnerApi.Environment environment,
      InstructionRequestHandler instructionHandler) {
    return new PassiveEnvironment(environment, instructionHandler);
  }

  private final RunnerApi.Environment environment;
  private final InstructionRequestHandler instructionHandler;

  private AtomicBoolean isClosed = new AtomicBoolean(false);

  private PassiveEnvironment(
      RunnerApi.Environment environment,
      InstructionRequestHandler instructionHandler) {
    this.environment = environment;
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

  /**
   * Closes this environment. The associated {@link InstructionRequestHandler} should
   * not be used after calling this.
   */
  @Override
  public void close() throws Exception {
    // The instruction handler should only be terminated once.
    // Do nothing if we have already requested termination.
    if (isClosed.compareAndSet(false, true)) {
      instructionHandler.close();
    }
  }
}

