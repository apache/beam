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

import com.google.auto.value.AutoValue;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;

/**
 * A handle to an available remote {@link Environment}. This environment is connected to a Fn API
 * Control service, and the associated client is available via {@link
 * #getInstructionRequestHandler()}.
 */
public interface RemoteEnvironment extends AutoCloseable {
  /**
   * Create a new {@link RemoteEnvironment} for the provided {@link Environment} and {@link
   * AutoCloseable} {@link InstructionRequestHandler}.
   *
   * <p>When the {@link RemoteEnvironment} is closed, the {@link InstructionRequestHandler} will be
   * closed.
   */
  static RemoteEnvironment forHandler(Environment env, InstructionRequestHandler handler) {
    return new AutoValue_RemoteEnvironment_SimpleRemoteEnvironment(env, handler);
  }

  /** Return the environment that the remote handles. */
  Environment getEnvironment();

  /** Return an {@link InstructionRequestHandler} which can communicate with the environment. */
  InstructionRequestHandler getInstructionRequestHandler();

  /**
   * {@inheritDoc}.
   *
   * <p>By default, closes the {@link #getInstructionRequestHandler()}.
   */
  @Override
  default void close() throws Exception {
    getInstructionRequestHandler().close();
  }

  /** A {@link RemoteEnvironment} which uses the default {@link #close()} behavior. */
  @AutoValue
  abstract class SimpleRemoteEnvironment implements RemoteEnvironment {
    @Override
    public abstract Environment getEnvironment();

    @Override
    public abstract InstructionRequestHandler getInstructionRequestHandler();
  }
}
