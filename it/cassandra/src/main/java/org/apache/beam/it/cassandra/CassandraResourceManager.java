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
package org.apache.beam.it.cassandra;

import static org.apache.beam.it.cassandra.CassandraResourceManagerUtils.generateKeyspaceName;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.common.utils.ExceptionUtils;
import org.apache.beam.it.testcontainers.TestContainerResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Client for managing Cassandra resources.
 *
 * <p>The class supports one database and multiple collections per database object. A database is
 * created when the first collection is created if one has not been created already.
 *
 * <p>The database name is formed using testId. The database name will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting.
 *
 * <p>The class is thread-safe.
 */
public class CassandraResourceManager extends TestContainerResourceManager<GenericContainer<?>>
    implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraResourceManager.class);

  private static final String DEFAULT_CASSANDRA_CONTAINER_NAME = "cassandra";

  // A list of available Cassandra Docker image tags can be found at
  // https://hub.docker.com/_/cassandra/tags
  private static final String DEFAULT_CASSANDRA_CONTAINER_TAG = "4.1.0";

  // 9042 is the default port that Cassandra is configured to listen on
  private static final int CASSANDRA_INTERNAL_PORT = 9042;

  private final CqlSession cassandraClient;
  private final String keyspaceName;
  private final boolean usingStaticDatabase;

  private CassandraResourceManager(Builder builder) {
    this(
        /* cassandraClient= */ null,
        new CassandraContainer<>(
            DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag)),
        builder);
  }

  @VisibleForTesting
  @SuppressWarnings("nullness")
  CassandraResourceManager(
      @Nullable CqlSession cassandraClient, CassandraContainer<?> container, Builder builder) {
    super(container, builder);

    this.usingStaticDatabase = builder.keyspaceName != null;
    this.keyspaceName =
        usingStaticDatabase ? builder.keyspaceName : generateKeyspaceName(builder.testId);
    this.cassandraClient =
        cassandraClient == null
            ? CqlSession.builder()
                .addContactPoint(
                    new InetSocketAddress(this.getHost(), this.getPort(CASSANDRA_INTERNAL_PORT)))
                .withLocalDatacenter("datacenter1")
                .build()
            : cassandraClient;

    if (!usingStaticDatabase) {
      // Keyspace request may timeout on a few environments, if Cassandra is warming up
      Failsafe.with(buildRetryPolicy())
          .run(
              () ->
                  this.cassandraClient.execute(
                      String.format(
                          "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}",
                          this.keyspaceName)));
    }
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  /** Returns the port to connect to the Cassandra Database. */
  public int getPort() {
    return super.getPort(CASSANDRA_INTERNAL_PORT);
  }

  /**
   * Returns the name of the Database that this Cassandra manager will operate in.
   *
   * @return the name of the Cassandra Database.
   */
  public synchronized String getKeyspaceName() {
    return keyspaceName;
  }

  /**
   * Execute the given statement on the managed keyspace.
   *
   * @param statement The statement to execute.
   * @return ResultSet from Cassandra.
   */
  public synchronized ResultSet executeStatement(String statement) {
    LOG.info("Executing statement: {}", statement);

    try {
      return Failsafe.with(buildRetryPolicy())
          .get(
              () ->
                  cassandraClient.execute(
                      SimpleStatement.newInstance(statement).setKeyspace(this.keyspaceName)));
    } catch (Exception e) {
      throw new CassandraResourceManagerException("Error reading collection.", e);
    }
  }

  /**
   * Inserts the given Document into a table.
   *
   * <p>A database will be created here, if one does not already exist.
   *
   * @param tableName The name of the table to insert the document into.
   * @param document The document to insert into the table.
   * @return A boolean indicating whether the Document was inserted successfully.
   */
  public synchronized boolean insertDocument(String tableName, Map<String, Object> document) {
    return insertDocuments(tableName, ImmutableList.of(document));
  }

  /**
   * Inserts the given Documents into a collection.
   *
   * <p>Note: Implementations may do collection creation here, if one does not already exist.
   *
   * @param tableName The name of the collection to insert the documents into.
   * @param documents A list of documents to insert into the collection.
   * @return A boolean indicating whether the Documents were inserted successfully.
   * @throws CassandraResourceManagerException if there is an error inserting the documents.
   */
  public synchronized boolean insertDocuments(String tableName, List<Map<String, Object>> documents)
      throws CassandraResourceManagerException {
    LOG.info(
        "Attempting to write {} documents to {}.{}.", documents.size(), keyspaceName, tableName);

    try {
      for (Map<String, Object> document : documents) {
        executeStatement(createInsertStatement(tableName, document));
      }
    } catch (Exception e) {
      throw new CassandraResourceManagerException("Error inserting documents.", e);
    }

    LOG.info("Successfully wrote {} documents to {}.{}", documents.size(), keyspaceName, tableName);

    return true;
  }

  /**
   * Reads all the Documents in a collection.
   *
   * @param tableName The name of the collection to read from.
   * @return An iterable of all the Documents in the collection.
   * @throws CassandraResourceManagerException if there is an error reading the collection.
   */
  public synchronized Iterable<Row> readTable(String tableName)
      throws CassandraResourceManagerException {
    LOG.info("Reading all documents from {}.{}", keyspaceName, tableName);

    Iterable<Row> documents;
    try {
      ResultSet resultSet = executeStatement(String.format("SELECT * FROM %s", tableName));
      documents = resultSet.all();
    } catch (Exception e) {
      throw new CassandraResourceManagerException("Error reading table.", e);
    }

    LOG.info("Successfully loaded documents from {}.{}", keyspaceName, tableName);

    return documents;
  }

  @Override
  public synchronized void cleanupAll() {
    LOG.info("Attempting to cleanup Cassandra manager.");

    boolean producedError = false;

    // First, delete the database if it was not given as a static argument
    if (!usingStaticDatabase) {
      try {
        executeStatement(String.format("DROP KEYSPACE IF EXISTS %s", this.keyspaceName));
      } catch (Exception e) {
        LOG.error("Failed to drop Cassandra keyspace {}.", keyspaceName, e);

        // Only bubble exception if the cause is not timeout or does not exist
        if (!ExceptionUtils.containsType(e, DriverTimeoutException.class)
            && !ExceptionUtils.containsMessage(e, "does not exist")) {
          producedError = true;
        }
      }
    }

    // Next, try to close the Cassandra client connection
    try {
      cassandraClient.close();
    } catch (Exception e) {
      LOG.error("Failed to delete Cassandra client.", e);
      producedError = true;
    }

    // Throw Exception at the end if there were any errors
    if (producedError) {
      throw new CassandraResourceManagerException(
          "Failed to delete resources. Check above for errors.");
    }

    super.cleanupAll();

    LOG.info("Cassandra manager successfully cleaned up.");
  }

  private String createInsertStatement(String tableName, Map<String, Object> map) {
    StringBuilder columns = new StringBuilder();
    StringBuilder values = new StringBuilder();

    for (Map.Entry<String, Object> entry : map.entrySet()) {
      columns.append(entry.getKey()).append(", ");

      // add quotes around strings
      if (entry.getValue() instanceof String) {
        values.append("'").append(entry.getValue()).append("'");
      } else {
        values.append(entry.getValue());
      }
      values.append(", ");
    }

    // Remove trailing comma and space
    if (!map.isEmpty()) {
      columns.delete(columns.length() - 2, columns.length());
      values.delete(values.length() - 2, values.length());
    }

    return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, values);
  }

  private static RetryPolicy<Object> buildRetryPolicy() {
    return RetryPolicy.builder()
        .withMaxRetries(5)
        .withDelay(Duration.ofSeconds(1))
        .handle(DriverTimeoutException.class)
        .build();
  }

  /** Builder for {@link CassandraResourceManager}. */
  public static final class Builder
      extends TestContainerResourceManager.Builder<CassandraResourceManager> {

    private @Nullable String keyspaceName;

    private Builder(String testId) {
      super(testId, DEFAULT_CASSANDRA_CONTAINER_NAME, DEFAULT_CASSANDRA_CONTAINER_TAG);
      this.keyspaceName = null;
    }

    /**
     * Sets the keyspace name to that of a static database instance. Use this method only when
     * attempting to operate on a pre-existing Cassandra database.
     *
     * <p>Note: if a database name is set, and a static Cassandra server is being used
     * (useStaticContainer() is also called on the builder), then a database will be created on the
     * static server if it does not exist, and it will not be removed when cleanupAll() is called on
     * the CassandraResourceManager.
     *
     * @param keyspaceName The database name.
     * @return this builder object with the database name set.
     */
    public Builder setKeyspaceName(String keyspaceName) {
      this.keyspaceName = keyspaceName;
      return this;
    }

    @Override
    public CassandraResourceManager build() {
      return new CassandraResourceManager(this);
    }
  }
}
