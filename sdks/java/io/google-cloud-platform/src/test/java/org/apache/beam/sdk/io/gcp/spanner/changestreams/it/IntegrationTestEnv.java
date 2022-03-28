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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.it;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTestEnv extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestEnv.class);
  private static final int TIMEOUT_MINUTES = 10;
  private static final int MAX_TABLE_NAME_LENGTH = 128;
  private static final int MAX_CHANGE_STREAM_NAME_LENGTH = 30;
  private static final int MAX_DATABASE_NAME_LENGTH = 30;
  private static final String METADATA_TABLE_NAME_PREFIX = "TestMetadata";
  private static final String SINGERS_TABLE_NAME_PREFIX = "Singers";
  private static final String CHANGE_STREAM_NAME_PREFIX = "SingersStream";
  private List<String> changeStreams;
  private List<String> tables;

  private String projectId;
  private String instanceId;
  private String databaseId;
  private String metadataTableName;
  private Spanner spanner;
  private DatabaseAdminClient databaseAdminClient;
  private DatabaseClient databaseClient;

  @Override
  protected void before() throws Throwable {
    final ChangeStreamTestPipelineOptions options =
        IOITHelper.readIOTestPipelineOptions(ChangeStreamTestPipelineOptions.class);

    projectId =
        Optional.ofNullable(options.getProjectId())
            .orElseGet(() -> options.as(GcpOptions.class).getProject());
    instanceId = options.getInstanceId();
    databaseId = generateDatabaseName(options.getDatabaseId());
    metadataTableName = generateTableName(METADATA_TABLE_NAME_PREFIX);
    spanner = SpannerOptions.newBuilder().setProjectId(projectId).build().getService();
    databaseAdminClient = spanner.getDatabaseAdminClient();

    recreateDatabase(databaseAdminClient, instanceId, databaseId);

    databaseClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

    changeStreams = new ArrayList<>();
    tables = new ArrayList<>();
  }

  @Override
  protected void after() {
    for (String changeStream : changeStreams) {
      try {
        databaseAdminClient
            .updateDatabaseDdl(
                instanceId,
                databaseId,
                Collections.singletonList("DROP CHANGE STREAM " + changeStream),
                null)
            .get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
      } catch (Exception e) {
        LOG.error("Failed to drop change stream " + changeStream + ". Skipping...", e);
      }
    }

    for (String table : tables) {
      try {
        databaseAdminClient
            .updateDatabaseDdl(
                instanceId, databaseId, Collections.singletonList("DROP TABLE " + table), null)
            .get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
      } catch (Exception e) {
        LOG.error("Failed to drop table " + table + ". Skipping...", e);
      }
    }

    try {
      databaseAdminClient.dropDatabase(instanceId, databaseId);
    } catch (Exception e) {
      LOG.error("Failed to drop database " + databaseId + ". Skipping...", e);
    }

    spanner.close();
  }

  String createSingersTable() throws InterruptedException, ExecutionException, TimeoutException {
    final String tableName = generateTableName(SINGERS_TABLE_NAME_PREFIX);
    LOG.info("Creating table " + tableName);
    databaseAdminClient
        .updateDatabaseDdl(
            instanceId,
            databaseId,
            Collections.singletonList(
                "CREATE TABLE "
                    + tableName
                    + " ("
                    + "   SingerId   INT64 NOT NULL,"
                    + "   FirstName  STRING(1024),"
                    + "   LastName   STRING(1024),"
                    + "   SingerInfo BYTES(MAX)"
                    + " ) PRIMARY KEY (SingerId)"),
            null)
        .get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
    tables.add(tableName);

    return tableName;
  }

  String createChangeStreamFor(String tableName)
      throws InterruptedException, ExecutionException, TimeoutException {
    final String changeStreamName = generateChangeStreamName();
    LOG.info("Creating change stream " + changeStreamName + " for " + tableName);
    databaseAdminClient
        .updateDatabaseDdl(
            instanceId,
            databaseId,
            Collections.singletonList(
                "CREATE CHANGE STREAM " + changeStreamName + " FOR " + tableName),
            null)
        .get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
    changeStreams.add(changeStreamName);

    return changeStreamName;
  }

  String getProjectId() {
    return projectId;
  }

  String getInstanceId() {
    return instanceId;
  }

  String getDatabaseId() {
    return databaseId;
  }

  String getMetadataTableName() {
    return metadataTableName;
  }

  DatabaseClient getDatabaseClient() {
    return databaseClient;
  }

  private void recreateDatabase(
      DatabaseAdminClient databaseAdminClient, String instanceId, String databaseId)
      throws ExecutionException, InterruptedException, TimeoutException {
    // Drops the database if it already exists
    databaseAdminClient.dropDatabase(instanceId, databaseId);
    LOG.info("Creating database " + databaseId);
    databaseAdminClient
        .createDatabase(instanceId, databaseId, Collections.emptyList())
        .get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
  }

  private String generateTableName(String prefix) {
    return prefix
        + "_"
        + RandomStringUtils.randomAlphanumeric(MAX_TABLE_NAME_LENGTH - 1 - prefix.length());
  }

  private String generateChangeStreamName() {
    return CHANGE_STREAM_NAME_PREFIX
        + "_"
        + RandomStringUtils.randomAlphanumeric(
            MAX_CHANGE_STREAM_NAME_LENGTH - 1 - CHANGE_STREAM_NAME_PREFIX.length());
  }

  private String generateDatabaseName(String prefix) {
    return prefix
        + "_"
        + RandomStringUtils.randomAlphanumeric(MAX_DATABASE_NAME_LENGTH - 1 - prefix.length())
            .toLowerCase(Locale.ROOT);
  }
}
