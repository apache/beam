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
package org.apache.beam.it.testcontainers;

import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.common.TestProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

/**
 * Abstract class for managing TestContainers resources in integration tests.
 *
 * <p>Resource managers that extend this class will use TestContainers as a backend to spin up
 * resources.
 *
 * <p>Optionally, a static resource can be specified by calling the useStaticContainer() method in
 * the {@link Builder} class. A static resource is a pre-configured database or other resource that
 * is ready to be connected to by the resource manager. This could be a pre-existing TestContainer
 * that has not been closed, a local database instance, a remote VM, or any other source that can be
 * connected to. If a static container is used, the host and port must also be configured using the
 * Builder's setHost() and setPort() methods, respectively.
 */
public abstract class TestContainerResourceManager<T extends GenericContainer<?>>
    implements ResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(TestContainerResourceManager.class);

  protected static final String HOST_IP = TestProperties.hostIp();
  private final T container;
  private final boolean usingStaticContainer;
  private final String host;
  protected int port;

  protected <B extends Builder<?>> TestContainerResourceManager(T container, B builder) {
    this(container, builder, null);
  }

  protected <B extends Builder<?>> TestContainerResourceManager(
      T container, B builder, @Nullable Callable<Void> setup) {
    this.container = container;
    this.usingStaticContainer = builder.useStaticContainer;
    this.host = builder.host == null ? HOST_IP : builder.host;
    this.port = builder.port;

    if (setup != null) {
      try {
        setup.call();
      } catch (Exception e) {
        throw new TestContainerResourceManagerException("Error running setup function.", e);
      }
    }

    if (!usingStaticContainer) {
      // TODO(pranavbhandari): Change this to use log.getUtf8StringWithoutLineEnding() when
      // testcontainers dependency is updated.
      container
          .withLogConsumer(
              log -> LOG.info("{}: {}", container.getDockerImageName(), log.getUtf8String()))
          .start();
    } else if (builder.host == null || builder.port < 0) {
      throw new TestContainerResourceManagerException(
          "This manager was configured to use a static resource, but the host and port were not properly set.");
    }
  }

  protected String getDockerImageName() {
    return container.getDockerImageName();
  }

  /**
   * Returns the host that the resource is running on. If a host was specified in the builder, then
   * this method will return that value. Otherwise, the host will default to localhost.
   *
   * @return the host that the resource is running on
   */
  public String getHost() {
    return host;
  }

  /**
   * Returns the port that the resource is actually mapped to on the host. When using a non-static
   * resource, the TestContainers framework will map the resource port to a random port on the host.
   * This method will return the mapped port that the resource traffic is routed through.
   *
   * <p>Note: When using a static container, the port that was specified in the builder will be
   * returned regardless of what is passed into this method as a parameter.
   *
   * @param mappedPort the port the resource was configured to listen on
   * @return the mapped port on the host
   */
  protected int getPort(int mappedPort) {
    if (port < 0) {
      return container.getMappedPort(mappedPort);
    }
    return port;
  }

  /**
   * Deletes all created resources (VM's, etc.) and stops the container, making the manager object
   * unusable.
   *
   * @throws TestContainerResourceManagerException if there is an error deleting the TestContainers
   *     resources.
   */
  @Override
  public void cleanupAll() throws TestContainerResourceManagerException {
    if (usingStaticContainer) {
      LOG.info("This manager was configured to use a static resource that will not be cleaned up.");
      return;
    }

    LOG.info("Attempting to cleanup TestContainers manager.");
    try {
      container.close();
      LOG.info("TestContainers manager successfully cleaned up.");
    } catch (Exception e) {
      throw new TestContainerResourceManagerException("Failed to close TestContainer resources", e);
    }
  }

  /** Builder for {@link TestContainerResourceManager}. */
  public abstract static class Builder<T extends TestContainerResourceManager<?>> {

    public String testId;
    public String containerImageName;
    public String containerImageTag;
    public @Nullable String host;
    public int port;
    public boolean useStaticContainer;

    public Builder(String testId, String containerImageName, String containerImageTag) {
      this.testId = testId;
      this.containerImageName = containerImageName;
      this.containerImageTag = containerImageTag;
      this.host = null;
      this.port = -1;
    }

    /**
     * Sets the name of the test container image. The tag is typically the version of the image.
     *
     * @param containerName The name of the container image.
     * @return this builder object with the image name set.
     */
    public Builder<T> setContainerImageName(String containerName) {
      this.containerImageName = containerName;
      return this;
    }

    /**
     * Sets the tag for the test container. The tag is typically the version of the image.
     *
     * @param containerTag The tag to use for the container.
     * @return this builder object with the tag set.
     */
    public Builder<T> setContainerImageTag(String containerTag) {
      this.containerImageTag = containerTag;
      return this;
    }

    /**
     * Sets the host of the resource that the resource manager will connect to. This will default to
     * localhost if not set.
     *
     * @param containerHost the resource host address.
     * @return this builder object with the host set.
     */
    public Builder<T> setHost(String containerHost) {
      this.host = containerHost;
      return this;
    }

    /**
     * Sets the port that the resource is hosted on.
     *
     * @param port the port the resource is hosted on.
     * @return this builder object with the port set.
     */
    public Builder<T> setPort(int port) {
      this.port = port;
      return this;
    }

    /**
     * Configures the resource manager to use a static resource instead of creating a new
     * TestContainer instance of the resource. This can be another TestContainer or any other VM or
     * server hosting the resource.
     *
     * <p>Note: When this option is enabled, the setPort() and setHost() methods must also be called
     * to configure the static resource address.
     *
     * @return this builder object with the useStaticContainer option enabled.
     */
    public Builder<T> useStaticContainer() {
      this.useStaticContainer = true;
      return this;
    }

    /**
     * Builds and returns a Resource Manager that extends TestContainerResourceManager.
     *
     * @return an instance of TestContainerResourceManager
     */
    public abstract T build();
  }
}
