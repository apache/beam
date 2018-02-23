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

import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;

/**
 * A {@link RemoteEnvironment} that talks to a Docker container. Accessors are thread-compatible.
 */
class DockerContainerEnvironment implements RemoteEnvironment {

  static DockerContainerEnvironment create(DockerWrapper docker,
      Environment environment, String containerId, SdkHarnessClient client) {
    return new DockerContainerEnvironment(docker, environment, containerId, client);
  }

  private final DockerWrapper docker;
  private final Environment environment;
  private final String containerId;
  private final SdkHarnessClient client;

  private DockerContainerEnvironment(DockerWrapper docker, Environment environment,
      String containerId, SdkHarnessClient client) {
    this.docker = docker;
    this.environment = environment;
    this.containerId = containerId;
    this.client = client;
  }

  @Override
  public Environment getEnvironment() {
    return environment;
  }

  @Override
  public SdkHarnessClient getClient() {
    return client;
  }

  /**
   * Closes this remote docker environment. The associated {@link SdkHarnessClient} must not be
   * used after calling this.
   */
  @Override
  public void close() throws Exception {
    getClient().close();
    docker.killContainer(containerId);
  }
}
