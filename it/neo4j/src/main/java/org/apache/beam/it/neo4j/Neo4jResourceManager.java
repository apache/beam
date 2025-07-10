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
package org.apache.beam.it.neo4j;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.generatePassword;
import static org.apache.beam.it.neo4j.Neo4jResourceManagerUtils.generateDatabaseName;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.testcontainers.TestContainerResourceManager;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Client for managing Neo4j resources.
 *
 * <p>The database name is formed using testId. The database name will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting.
 *
 * <p>The class is thread-safe.
 */
public class Neo4jResourceManager extends TestContainerResourceManager<Neo4jContainer<?>>
    implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jResourceManager.class);

  private static final String DEFAULT_NEO4J_CONTAINER_NAME = "neo4j";

  // A list of available Neo4j Docker image tags can be found at
  // https://hub.docker.com/_/neo4j/tags
  private static final String DEFAULT_NEO4J_CONTAINER_TAG = "5-enterprise";

  // 7687 is the default Bolt port that Neo4j is configured to listen on
  private static final int NEO4J_BOLT_PORT = 7687;

  private final Driver neo4jDriver;
  private final String databaseName;
  private final DatabaseWaitOption waitOption;
  private final String connectionString;
  private final boolean usingStaticDatabase;

  private final String adminPassword;

  private Neo4jResourceManager(Builder builder) {
    this(
        builder.driver,
        new Neo4jContainer<>(
                DockerImageName.parse(builder.containerImageName)
                    .withTag(builder.containerImageTag))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withAdminPassword(builder.adminPassword),
        builder);
  }

  @VisibleForTesting
  @SuppressWarnings("nullness")
  Neo4jResourceManager(@Nullable Driver neo4jDriver, Neo4jContainer<?> container, Builder builder) {
    super(container, builder);

    this.adminPassword = builder.adminPassword;
    this.connectionString =
        String.format("neo4j://%s:%d", this.getHost(), this.getPort(NEO4J_BOLT_PORT));
    this.neo4jDriver =
        neo4jDriver == null
            ? GraphDatabase.driver(connectionString, AuthTokens.basic("neo4j", this.adminPassword))
            : neo4jDriver;
    this.usingStaticDatabase = builder.databaseName != null;
    if (usingStaticDatabase) {
      this.databaseName = builder.databaseName;
      this.waitOption = null;
    } else {
      this.databaseName = generateDatabaseName(builder.testId);
      this.waitOption = builder.waitOption;
      createDatabase(databaseName, waitOption);
    }
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  /** Returns the URI connection string to the Neo4j Database. */
  public synchronized String getUri() {
    return connectionString;
  }

  public List<Map<String, Object>> run(String query) {
    return this.run(query, Collections.emptyMap());
  }

  public List<Map<String, Object>> run(String query, Map<String, Object> parameters) {
    try (Session session =
        neo4jDriver.session(SessionConfig.builder().withDatabase(databaseName).build())) {
      return session.run(query, parameters).list(record -> record.asMap());
    } catch (Exception e) {
      throw new Neo4jResourceManagerException(String.format("Error running query %s.", query), e);
    }
  }

  /**
   * Returns the name of the Database that this Neo4j manager will operate in.
   *
   * @return the name of the Neo4j Database.
   */
  public synchronized String getDatabaseName() {
    return databaseName;
  }

  @Override
  public synchronized void cleanupAll() {
    LOG.info("Attempting to clean up Neo4j manager.");

    boolean producedError = false;

    // First, delete the database if it was not given as a static argument
    try {
      if (!usingStaticDatabase) {
        dropDatabase(databaseName, waitOption);
      }
    } catch (Exception e) {
      LOG.error("Failed to delete Neo4j database {}.", databaseName, e);
      producedError = true;
    }

    // Next, try to close the Neo4j client connection
    try {
      neo4jDriver.close();
    } catch (Exception e) {
      LOG.error("Failed to delete Neo4j client.", e);
      producedError = true;
    }

    // Throw Exception at the end if there were any errors
    if (producedError) {
      throw new Neo4jResourceManagerException(
          "Failed to delete resources. Check above for errors.");
    }

    super.cleanupAll();

    LOG.info("Neo4j manager successfully cleaned up.");
  }

  private void createDatabase(String databaseName, DatabaseWaitOption waitOption) {
    try (Session session =
        neo4jDriver.session(SessionConfig.builder().withDatabase("system").build())) {
      String query =
          String.format("CREATE DATABASE $db %s", DatabaseWaitOptions.asCypher(waitOption));
      session.run(query, Collections.singletonMap("db", databaseName)).consume();
    } catch (Exception e) {
      throw new Neo4jResourceManagerException(
          String.format("Error dropping database %s.", databaseName), e);
    }
  }

  @VisibleForTesting
  void dropDatabase(String databaseName, DatabaseWaitOption waitOption) {
    try (Session session =
        neo4jDriver.session(SessionConfig.builder().withDatabase("system").build())) {
      String query =
          String.format("DROP DATABASE $db %s", DatabaseWaitOptions.asCypher(waitOption));
      session.run(query, Collections.singletonMap("db", databaseName)).consume();
    } catch (Exception e) {
      throw new Neo4jResourceManagerException(
          String.format("Error dropping database %s.", databaseName), e);
    }
  }

  public String getAdminPassword() {
    return adminPassword;
  }

  /** Builder for {@link Neo4jResourceManager}. */
  public static final class Builder
      extends TestContainerResourceManager.Builder<Neo4jResourceManager> {

    private @Nullable String databaseName;
    private @Nullable DatabaseWaitOption waitOption;

    private String adminPassword;

    private @Nullable Driver driver;

    private Builder(String testId) {
      super(testId, DEFAULT_NEO4J_CONTAINER_NAME, DEFAULT_NEO4J_CONTAINER_TAG);
      this.adminPassword = generatePassword(4, 10, 2, 2, 0, Collections.emptyList());
      this.databaseName = null;
      this.waitOption = null;
      this.driver = null;
    }

    /**
     * Sets the database name to that of a static database instance. Use this method only when
     * attempting to operate on a pre-existing Neo4j database.
     *
     * <p>Note: if a database name is set, and a static Neo4j server is being used
     * (useStaticContainer() is also called on the builder), then a database will be created on the
     * static server if it does not exist, and it will not be removed when cleanupAll() is called on
     * the Neo4jResourceManager.
     *
     * @param databaseName The database name.
     * @return this builder object with the database name set.
     */
    public Builder setDatabaseName(String databaseName) {
      return setDatabaseName(databaseName, DatabaseWaitOptions.noWaitDatabase());
    }

    /**
     * Sets the database name to that of a static database instance and sets the wait policy. Use
     * this method only when attempting to operate on a pre-existing Neo4j database.
     *
     * <p>Note: if a database name is set, and a static Neo4j server is being used
     * (useStaticContainer() is also called on the builder), then a database will be created on the
     * static server if it does not exist, and it will not be removed when cleanupAll() is called on
     * the Neo4jResourceManager.
     *
     * <p>{@link DatabaseWaitOptions} exposes all configurable wait options
     *
     * @param databaseName The database name.
     * @param waitOption The database wait policy.
     * @return this builder object with the database name set.
     */
    public Builder setDatabaseName(String databaseName, DatabaseWaitOption waitOption) {
      this.databaseName = databaseName;
      this.waitOption = waitOption;
      return this;
    }

    public Builder setAdminPassword(String password) {
      this.adminPassword = password;
      return this;
    }

    @VisibleForTesting
    Builder setDriver(Driver driver) {
      this.driver = driver;
      return this;
    }

    @Override
    public Neo4jResourceManager build() {
      return new Neo4jResourceManager(this);
    }
  }
}
