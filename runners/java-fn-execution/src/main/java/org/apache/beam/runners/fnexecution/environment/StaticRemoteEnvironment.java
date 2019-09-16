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

import net.jcip.annotations.ThreadSafe;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;

/** A {@link RemoteEnvironment} that connects to Dataflow runner harness. */
@ThreadSafe
public class StaticRemoteEnvironment implements RemoteEnvironment {

  static StaticRemoteEnvironment create(
      Environment environment, InstructionRequestHandler instructionRequestHandler) {
    return new StaticRemoteEnvironment(environment, instructionRequestHandler);
  }

  private final Environment environment;
  private final InstructionRequestHandler instructionRequestHandler;

  private boolean isClosed = false;

  private StaticRemoteEnvironment(
      Environment environment, InstructionRequestHandler instructionRequestHandler) {
    this.environment = environment;
    this.instructionRequestHandler = instructionRequestHandler;
  }

  @Override
  public Environment getEnvironment() {
    return this.environment;
  }

  @Override
  public InstructionRequestHandler getInstructionRequestHandler() {
    return this.instructionRequestHandler;
  }

  @Override
  public synchronized void close() throws Exception {
    // The instruction handler should each only be terminated once.
    // Do nothing if we have already requested termination.
    if (!isClosed) {
      return;
    }
    isClosed = true;
    this.instructionRequestHandler.close();
  }
}
