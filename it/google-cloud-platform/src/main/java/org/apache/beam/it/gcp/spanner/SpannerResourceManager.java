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
package org.apache.beam.it.gcp.spanner;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.checkValidProjectId;
import static org.apache.beam.it.common.utils.ResourceManagerUtils.generateNewId;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.common.utils.ExceptionUtils;
import org.apache.beam.it.gcp.spanner.utils.SpannerResourceManagerUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for managing Spanner resources.
 *
 * <p>The class supports one instance, one database, and multiple tables per manager object. The
 * instance and database are created when the first table is created.
 *
 * <p>The instance and database ids are formed using testId. The database id will be {testId}, with
 * some extra formatting. The instance id will be "{testId}-{ISO8601 time, microsecond precision}",
 * with additional formatting. Note: If testId is more than 30 characters, a new testId will be
 * formed for naming: {first 21 chars of long testId} + “-” + {8 char hash of testId}.
 *
 * <p>The class is thread-safe.
 */
public final class SpannerResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerResourceManager.class);
  private static final int MAX_BASE_ID_LENGTH = 30;
  private static final int DEFAULT_NODE_COUNT = 1;

  // Retry settings for instance creation
  private static final int CREATE_MAX_RETRIES = 5;
  private static final Duration CREATE_BACKOFF_DELAY = Duration.ofSeconds(10);
  private static final Duration CREATE_BACKOFF_MAX_DELAY = Duration.ofSeconds(60);
  private static final double CREATE_BACKOFF_JITTER = 0.1;

  private boolean hasInstance = false;
  private boolean hasDatabase = false;

  private final String projectId;
  private final String instanceId;
  private final boolean usingStaticInstance;
  private final String databaseId;
  private final String region;
  private final int nodeCount;

  private final Dialect dialect;

  private final Spanner spanner;
  private final InstanceAdminClient instanceAdminClient;
  private final DatabaseAdminClient databaseAdminClient;

  private SpannerResourceManager(Builder builder) {
    this(
        SpannerOptions.newBuilder().setProjectId(builder.projectId).build().getService(),
        builder.testId,
        builder.projectId,
        builder.region,
        builder.dialect,
        builder.useStaticInstance,
        builder.instanceId,
        builder.nodeCount);
  }

  @VisibleForTesting
  SpannerResourceManager(
      Spanner spanner,
      String testId,
      String projectId,
      String region,
      Dialect dialect,
      boolean useStaticInstance,
      @Nullable String instanceId,
      int nodeCount) {
    // Check that the project ID conforms to GCP standards
    checkValidProjectId(projectId);

    if (testId.length() > MAX_BASE_ID_LENGTH) {
      testId = generateNewId(testId, MAX_BASE_ID_LENGTH);
    }
    this.projectId = projectId;

    if (useStaticInstance) {
      if (instanceId == null) {
        throw new SpannerResourceManagerException(
            "This manager was configured to use a static resource, but the instanceId was not properly set.");
      }
      this.instanceId = instanceId;
    } else {
      this.instanceId = SpannerResourceManagerUtils.generateInstanceId(testId);
    }
    this.usingStaticInstance = useStaticInstance;
    this.databaseId = SpannerResourceManagerUtils.generateDatabaseId(testId);
    this.region = region;
    this.dialect = dialect;
    this.spanner = spanner;
    this.nodeCount = nodeCount;
    this.instanceAdminClient = spanner.getInstanceAdminClient();
    this.databaseAdminClient = spanner.getDatabaseAdminClient();
  }

  public static Builder builder(String testId, String projectId, String region) {
    return new Builder(testId, projectId, region, Dialect.GOOGLE_STANDARD_SQL, DEFAULT_NODE_COUNT);
  }

  public static Builder builder(String testId, String projectId, String region, int nodeCount) {
    return new Builder(testId, projectId, region, Dialect.GOOGLE_STANDARD_SQL, nodeCount);
  }

  public static Builder builder(String testId, String projectId, String region, Dialect dialect) {
    return new Builder(testId, projectId, region, dialect, DEFAULT_NODE_COUNT);
  }

  private synchronized void maybeCreateInstance() {
    checkIsUsable();

    if (usingStaticInstance) {
      LOG.info("Not creating Spanner instance - reusing static {}", instanceId);
      hasInstance = true;
      return;
    }

    if (hasInstance) {
      return;
    }

    LOG.info("Creating instance {} in project {}.", instanceId, projectId);
    try {
      InstanceInfo instanceInfo =
          InstanceInfo.newBuilder(InstanceId.of(projectId, instanceId))
              .setInstanceConfigId(InstanceConfigId.of(projectId, "regional-" + region))
              .setDisplayName(instanceId)
              .setNodeCount(nodeCount)
              .build();

      // Retry creation if there's a quota error
      Instance instance =
          Failsafe.with(retryOnQuotaException())
              .get(() -> instanceAdminClient.createInstance(instanceInfo).get());

      hasInstance = true;
      LOG.info("Successfully created instance {}: {}.", instanceId, instance.getState());
    } catch (Exception e) {
      cleanupAll();
      throw new SpannerResourceManagerException("Failed to create instance.", e);
    }
  }

  private synchronized void maybeCreateDatabase() {
    checkIsUsable();
    if (hasDatabase) {
      return;
    }
    LOG.info("Creating database {} in instance {}.", databaseId, instanceId);

    try {
      Database database =
          Failsafe.with(retryOnQuotaException())
              .get(
                  () ->
                      databaseAdminClient
                          .createDatabase(
                              databaseAdminClient
                                  .newDatabaseBuilder(
                                      DatabaseId.of(projectId, instanceId, databaseId))
                                  .setDialect(dialect)
                                  .build(),
                              ImmutableList.of())
                          .get());

      hasDatabase = true;
      LOG.info("Successfully created database {}: {}.", databaseId, database.getState());
    } catch (Exception e) {
      cleanupAll();
      throw new SpannerResourceManagerException("Failed to create database.", e);
    }
  }

  private static <T> RetryPolicy<T> retryOnQuotaException() {
    return RetryPolicy.<T>builder()
        .handleIf(exception -> ExceptionUtils.containsMessage(exception, "RESOURCE_EXHAUSTED"))
        .withMaxRetries(CREATE_MAX_RETRIES)
        .withBackoff(CREATE_BACKOFF_DELAY, CREATE_BACKOFF_MAX_DELAY)
        .withJitter(CREATE_BACKOFF_JITTER)
        .build();
  }

  private void checkIsUsable() throws IllegalStateException {
    if (spanner.isClosed()) {
      throw new IllegalStateException("Manager has cleaned up all resources and is unusable.");
    }
  }

  private void checkHasInstanceAndDatabase() throws IllegalStateException {
    if (!hasInstance) {
      throw new IllegalStateException("There is no instance for manager to perform operation on.");
    }
    if (!hasDatabase) {
      throw new IllegalStateException("There is no database for manager to perform operation on");
    }
  }

  /**
   * Return the instance ID this Resource Manager uses to create and manage tables in.
   *
   * @return the instance ID.
   */
  public String getInstanceId() {
    return this.instanceId;
  }

  /**
   * Return the dataset ID this Resource Manager uses to create and manage tables in.
   *
   * @return the dataset ID.
   */
  public String getDatabaseId() {
    return this.databaseId;
  }

  /**
   * Executes a DDL statement.
   *
   * <p>Note: Implementations may do instance creation and database creation here.
   *
   * @param statement The DDL statement.
   * @throws IllegalStateException if method is called after resources have been cleaned up.
   */
  public synchronized void executeDdlStatement(String statement) throws IllegalStateException {
    checkIsUsable();
    maybeCreateInstance();
    maybeCreateDatabase();

    LOG.info("Executing DDL statement '{}' on database {}.", statement, databaseId);
    try {
      databaseAdminClient
          .updateDatabaseDdl(
              instanceId, databaseId, ImmutableList.of(statement), /* operationId= */ null)
          .get();
      LOG.info("Successfully executed DDL statement '{}' on database {}.", statement, databaseId);
    } catch (ExecutionException | InterruptedException | SpannerException e) {
      throw new SpannerResourceManagerException("Failed to execute statement.", e);
    }
  }

  /**
   * Writes a given record into a table. This method requires {@link
   * SpannerResourceManager#executeDdlStatement(String)} to be called for the target table
   * beforehand.
   *
   * @param tableRecord A mutation object representing the table record.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  public synchronized void write(Mutation tableRecord) throws IllegalStateException {
    write(ImmutableList.of(tableRecord));
  }

  /**
   * Writes a collection of table records into one or more tables. This method requires {@link
   * SpannerResourceManager#executeDdlStatement(String)} to be called for the target table
   * beforehand.
   *
   * @param tableRecords A collection of mutation objects representing table records.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  public synchronized void write(Iterable<Mutation> tableRecords) throws IllegalStateException {
    checkIsUsable();
    checkHasInstanceAndDatabase();

    LOG.info("Sending {} mutations to {}.{}", Iterables.size(tableRecords), instanceId, databaseId);
    try {
      DatabaseClient databaseClient =
          spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
      databaseClient.write(tableRecords);
      LOG.info("Successfully sent mutations to {}.{}", instanceId, databaseId);
    } catch (SpannerException e) {
      throw new SpannerResourceManagerException("Failed to write mutations.", e);
    }
  }

  /**
   * Reads all the rows in a table. This method requires {@link
   * SpannerResourceManager#executeDdlStatement(String)} to be called for the target table
   * beforehand.
   *
   * @param tableId The id of the table to read rows from.
   * @param columnNames The table's column names.
   * @return A ResultSet object containing all the rows in the table.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  public synchronized ImmutableList<Struct> readTableRecords(String tableId, String... columnNames)
      throws IllegalStateException {
    return readTableRecords(tableId, ImmutableList.copyOf(columnNames));
  }

  /**
   * Reads all the rows in a table.This method requires {@link
   * SpannerResourceManager#executeDdlStatement(String)} to be called for the target table
   * beforehand.
   *
   * @param tableId The id of table to read rows from.
   * @param columnNames A collection of the table's column names.
   * @return A ResultSet object containing all the rows in the table.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  public synchronized ImmutableList<Struct> readTableRecords(
      String tableId, Iterable<String> columnNames) throws IllegalStateException {
    checkIsUsable();
    checkHasInstanceAndDatabase();

    LOG.info(
        "Loading columns {} from {}.{}.{}",
        Iterables.toString(columnNames),
        instanceId,
        databaseId,
        tableId);
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

    try (ReadContext readContext = databaseClient.singleUse();
        ResultSet resultSet = readContext.read(tableId, KeySet.all(), columnNames)) {
      ImmutableList.Builder<Struct> tableRecordsBuilder = ImmutableList.builder();

      while (resultSet.next()) {
        tableRecordsBuilder.add(resultSet.getCurrentRowAsStruct());
      }
      ImmutableList<Struct> tableRecords = tableRecordsBuilder.build();
      LOG.info(
          "Loaded {} records from {}.{}.{}", tableRecords.size(), instanceId, databaseId, tableId);
      return tableRecords;
    } catch (SpannerException e) {
      throw new SpannerResourceManagerException("Error occurred while reading table records.", e);
    }
  }

  /**
   * Deletes all created resources (instance, database, and tables) and cleans up all Spanner
   * sessions, making the manager object unusable.
   */
  @Override
  public synchronized void cleanupAll() {
    try {

      if (usingStaticInstance) {
        if (databaseAdminClient != null) {
          Failsafe.with(retryOnQuotaException())
              .run(() -> databaseAdminClient.dropDatabase(instanceId, databaseId));
        }
      } else {
        LOG.info("Deleting instance {}...", instanceId);

        if (instanceAdminClient != null) {
          Failsafe.with(retryOnQuotaException())
              .run(() -> instanceAdminClient.deleteInstance(instanceId));
        }

        hasInstance = false;
      }

      hasDatabase = false;
    } catch (SpannerException e) {
      throw new SpannerResourceManagerException("Failed to delete instance.", e);
    } finally {
      if (!spanner.isClosed()) {
        spanner.close();
      }
    }
    LOG.info("Manager successfully cleaned up.");
  }

  /** Builder for {@link SpannerResourceManager}. */
  public static final class Builder {

    private final String testId;
    private final String projectId;
    private final String region;
    private boolean useStaticInstance;
    private @Nullable String instanceId;
    private final int nodeCount;

    private final Dialect dialect;

    private Builder(
        String testId, String projectId, String region, Dialect dialect, int nodeCount) {
      this.testId = testId;
      this.projectId = projectId;
      this.region = region;
      this.dialect = dialect;
      this.instanceId = null;
      this.nodeCount = nodeCount;
    }

    /**
     * Configures the resource manager to use a static GCP resource instead of creating a new
     * instance of the resource.
     *
     * @return this builder object with the useStaticInstance option enabled.
     */
    public Builder useStaticInstance() {
      this.useStaticInstance = true;
      return this;
    }

    /**
     * Looks at the system properties if there's an instance id, and reuses it if configured.
     *
     * @return this builder object with the useStaticInstance option enabled and instance set if
     *     configured, the same builder otherwise.
     */
    @SuppressWarnings("nullness")
    public Builder maybeUseStaticInstance() {
      if (System.getProperty("spannerInstanceId") != null) {
        this.useStaticInstance = true;
        this.instanceId = System.getProperty("spannerInstanceId");
      }
      return this;
    }

    public SpannerResourceManager build() {
      return new SpannerResourceManager(this);
    }
  }
}
