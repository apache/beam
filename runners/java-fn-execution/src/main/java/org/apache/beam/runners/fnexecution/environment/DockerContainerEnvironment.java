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

import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RemoteEnvironment} that wraps a running Docker container.
 *
 * <p>A {@link DockerContainerEnvironment} owns both the underlying docker container that it
 * communicates with an the {@link InstructionRequestHandler} that it uses to do so.
 */
@ThreadSafe
class DockerContainerEnvironment implements RemoteEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(DockerContainerEnvironment.class);

  static DockerContainerEnvironment create(
      DockerCommand docker,
      Environment environment,
      String containerId,
      InstructionRequestHandler instructionHandler,
      boolean retainDockerContainer) {
    return new DockerContainerEnvironment(
        docker, environment, containerId, instructionHandler, retainDockerContainer);
  }

  private final Object lock = new Object();
  private final DockerCommand docker;
  private final Environment environment;
  private final String containerId;
  private final InstructionRequestHandler instructionHandler;
  private final boolean retainDockerContainer;

  private boolean isClosed = false;

  private DockerContainerEnvironment(
      DockerCommand docker,
      Environment environment,
      String containerId,
      InstructionRequestHandler instructionHandler,
      boolean retainDockerContainer) {
    this.docker = docker;
    this.environment = environment;
    this.containerId = containerId;
    this.instructionHandler = instructionHandler;
    this.retainDockerContainer = retainDockerContainer;
  }

  @Override
  public Environment getEnvironment() {
    return environment;
  }

  @Override
  public InstructionRequestHandler getInstructionRequestHandler() {
    return instructionHandler;
  }

  /**
   * Closes this remote docker environment. The associated {@link InstructionRequestHandler} should
   * not be used after calling this.
   */
  @Override
  public void close() throws Exception {
    synchronized (lock) {
      // The running docker container and instruction handler should each only be terminated once.
      // Do nothing if we have already requested termination.
      if (!isClosed) {
        isClosed = true;
        instructionHandler.close();
        String containerLogs = docker.getContainerLogs(containerId);
        LOG.info("Closing Docker container {}. Logs:\n{}", containerId, containerLogs);
        docker.killContainer(containerId);
        if (!retainDockerContainer) {
          docker.removeContainer(containerId);
        }
      }
    }
  }
}
