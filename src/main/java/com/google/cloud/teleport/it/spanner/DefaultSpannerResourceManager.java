/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.spanner;

import static com.google.cloud.teleport.it.spanner.SpannerResourceManagerUtils.generateDatabaseId;
import static com.google.cloud.teleport.it.spanner.SpannerResourceManagerUtils.generateInstanceId;
import static com.google.cloud.teleport.it.spanner.SpannerResourceManagerUtils.generateNewId;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default class for implementation of {@link SpannerResourceManager} interface.
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
public final class DefaultSpannerResourceManager implements SpannerResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultSpannerResourceManager.class);
  private static final int MAX_BASE_ID_LENGTH = 30;

  private boolean hasInstance = false;
  private boolean hasDatabase = false;

  private final String projectId;
  private final String instanceId;
  private final String databaseId;
  private final String region;

  private final Spanner spanner;
  private final InstanceAdminClient instanceAdminClient;
  private final DatabaseAdminClient databaseAdminClient;

  private DefaultSpannerResourceManager(Builder builder) {
    this(
        SpannerOptions.newBuilder().setProjectId(builder.projectId).build().getService(),
        builder.testId,
        builder.projectId,
        builder.region);
  }

  @VisibleForTesting
  DefaultSpannerResourceManager(Spanner spanner, String testId, String projectId, String region) {
    if (testId.length() > MAX_BASE_ID_LENGTH) {
      testId = generateNewId(testId, MAX_BASE_ID_LENGTH);
    }
    this.projectId = projectId;
    this.instanceId = generateInstanceId(testId);
    this.databaseId = generateDatabaseId(testId);

    this.region = region;
    this.spanner = spanner;
    this.instanceAdminClient = spanner.getInstanceAdminClient();
    this.databaseAdminClient = spanner.getDatabaseAdminClient();
  }

  public static Builder builder(String testId, String projectId, String region) {
    return new Builder(testId, projectId, region);
  }

  private synchronized void maybeCreateInstance() {
    checkIsUsable();
    if (hasInstance) {
      return;
    }
    LOG.info("Creating instance {} in project {}.", instanceId, projectId);
    InstanceInfo instanceInfo =
        InstanceInfo.newBuilder(InstanceId.of(projectId, instanceId))
            .setInstanceConfigId(InstanceConfigId.of(projectId, region))
            .setDisplayName(instanceId)
            .setNodeCount(1)
            .build();
    try {
      instanceAdminClient.createInstance(instanceInfo).get();
      hasInstance = true;
      LOG.info("Successfully created instance {}.", instanceId);
    } catch (ExecutionException | InterruptedException | SpannerException e) {
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
      databaseAdminClient.createDatabase(instanceId, databaseId, ImmutableList.of()).get();
      hasDatabase = true;
      LOG.info("Successfully created database {}.", databaseId);
    } catch (ExecutionException | InterruptedException | SpannerException e) {
      cleanupAll();
      throw new SpannerResourceManagerException("Failed to create database.", e);
    }
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

  /** Creates an instance and database as well if there are none at the time of method call. */
  @Override
  public synchronized void createTable(String statement) throws IllegalStateException {
    checkIsUsable();
    maybeCreateInstance();
    maybeCreateDatabase();

    LOG.info("Creating table in database {} using statement '{}'.", databaseId, statement);
    try {
      databaseAdminClient
          .updateDatabaseDdl(
              instanceId, databaseId, ImmutableList.of(statement), /* operationId= */ null)
          .get();
      LOG.info("Successfully created table in database {}.", databaseId);
    } catch (ExecutionException | InterruptedException | SpannerException e) {
      throw new SpannerResourceManagerException("Failed to create table.", e);
    }
  }

  @Override
  public synchronized void write(Mutation tableRecord) throws IllegalStateException {
    write(ImmutableList.of(tableRecord));
  }

  @Override
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

  @Override
  public synchronized ImmutableList<Struct> readTableRecords(String tableId, String... columnNames)
      throws IllegalStateException {
    return readTableRecords(tableId, ImmutableList.copyOf(columnNames));
  }

  @Override
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

    try (ResultSet resultSet =
        databaseClient.singleUse().read(tableId, KeySet.all(), columnNames)) {
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

  @Override
  public synchronized void cleanupAll() {
    LOG.info("Attempting to cleanup manager.");
    try {
      instanceAdminClient.deleteInstance(instanceId);
      hasInstance = false;
      hasDatabase = false;
    } catch (SpannerException e) {
      throw new SpannerResourceManagerException("Failed to delete instance.", e);
    } finally {
      spanner.close();
    }
    LOG.info("Manager successfully cleaned up.");
  }

  /** Builder for {@link DefaultSpannerResourceManager}. */
  public static final class Builder {

    private final String testId;
    private final String projectId;
    private final String region;

    private Builder(String testId, String projectId, String region) {
      this.testId = testId;
      this.projectId = projectId;
      this.region = region;
    }

    public DefaultSpannerResourceManager build() {
      return new DefaultSpannerResourceManager(this);
    }
  }
}
