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
package com.google.cloud.teleport.it.bigtable;

import static com.google.cloud.teleport.it.bigtable.BigtableResourceManagerUtils.checkValidTableId;
import static com.google.cloud.teleport.it.bigtable.BigtableResourceManagerUtils.generateDefaultClusters;
import static com.google.cloud.teleport.it.bigtable.BigtableResourceManagerUtils.generateInstanceId;
import static com.google.cloud.teleport.it.common.ResourceManagerUtils.checkValidProjectId;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateInstanceRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * Default class for implementation of {@link BigtableResourceManager} interface.
 *
 * <p>The class supports one instance, and multiple tables per manager object. An instance is
 * created when the first table is created if one has not been created already.
 *
 * <p>The instance id is formed using testId. The instance id will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting. Note: If testId is more than 30 characters,
 * a new testId will be formed for naming: {first 21 chars of long testId} + “-” + {8 char hash of
 * testId}.
 *
 * <p>The class is thread-safe.
 */
public class DefaultBigtableResourceManager implements BigtableResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultBigtableResourceManager.class);
  private static final String DEFAULT_CLUSTER_ZONE = "us-central1-a";
  private static final int DEFAULT_CLUSTER_NUM_NODES = 1;
  private static final StorageType DEFAULT_CLUSTER_STORAGE_TYPE = StorageType.SSD;

  private final String projectId;
  private final String instanceId;
  private final BigtableResourceManagerClientFactory bigtableResourceManagerClientFactory;

  private boolean hasInstance = false;

  private DefaultBigtableResourceManager(DefaultBigtableResourceManager.Builder builder)
      throws IOException {
    // Check that the project ID conforms to GCP standards
    checkValidProjectId(builder.projectId);

    // generate instance id based on given test id.
    this.instanceId = generateInstanceId(builder.testId);

    // create the bigtable admin and data client settings builders, and set the necessary id's for
    // each.
    BigtableInstanceAdminSettings.Builder bigtableInstanceAdminSettings =
        BigtableInstanceAdminSettings.newBuilder().setProjectId(builder.projectId);
    BigtableTableAdminSettings.Builder bigtableTableAdminSettings =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(builder.projectId)
            .setInstanceId(this.instanceId);
    BigtableDataSettings.Builder bigtableDataSettings =
        BigtableDataSettings.newBuilder()
            .setProjectId(builder.projectId)
            .setInstanceId(this.instanceId);

    // add the credentials to the builders, if set.
    if (builder.credentialsProvider != null) {
      bigtableInstanceAdminSettings.setCredentialsProvider(builder.credentialsProvider);
      bigtableTableAdminSettings.setCredentialsProvider(builder.credentialsProvider);
      bigtableDataSettings.setCredentialsProvider(builder.credentialsProvider);
    }

    this.projectId = builder.projectId;
    this.bigtableResourceManagerClientFactory =
        new BigtableResourceManagerClientFactory(
            bigtableInstanceAdminSettings.build(),
            bigtableTableAdminSettings.build(),
            bigtableDataSettings.build());
  }

  @VisibleForTesting
  DefaultBigtableResourceManager(
      String testId,
      String projectId,
      BigtableResourceManagerClientFactory bigtableResourceManagerClientFactory) {
    this.projectId = projectId;
    this.instanceId = generateInstanceId(testId);
    this.bigtableResourceManagerClientFactory = bigtableResourceManagerClientFactory;
  }

  public static DefaultBigtableResourceManager.Builder builder(String testId, String projectId)
      throws IOException {
    return new DefaultBigtableResourceManager.Builder(testId, projectId);
  }

  /**
   * Returns the project ID this Resource Manager is configured to operate on.
   *
   * @return the project ID.
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * Return the instance ID this Resource Manager uses to create and manage tables in.
   *
   * @return the instance ID.
   */
  public String getInstanceId() {
    return instanceId;
  }

  @Override
  public synchronized void createInstance(Iterable<BigtableResourceManagerCluster> clusters) {

    // Check to see if instance already exists, and throw error if it does
    if (hasInstance) {
      throw new IllegalStateException(
          "Instance " + instanceId + " already exists for project " + projectId + ".");
    }

    LOG.info("Creating instance {} in project {}.", instanceId, projectId);

    // Create instance request object and add all the given clusters to the request
    CreateInstanceRequest request = CreateInstanceRequest.of(instanceId);
    for (BigtableResourceManagerCluster cluster : clusters) {
      request.addCluster(
          cluster.clusterId(), cluster.zone(), cluster.numNodes(), cluster.storageType());
    }

    // Send the instance request to Google Cloud
    try (BigtableInstanceAdminClient instanceAdminClient =
        bigtableResourceManagerClientFactory.bigtableInstanceAdminClient()) {
      instanceAdminClient.createInstance(request);
    } catch (Exception e) {
      throw new BigtableResourceManagerException(
          "Failed to create instance " + instanceId + ".", e);
    }
    hasInstance = true;

    LOG.info("Successfully created instance {}.", instanceId);
  }

  /**
   * Helper method for determining if an instance has been created for the ResourceManager object.
   *
   * @throws IllegalStateException if an instance has not yet been created.
   */
  private void checkHasInstance() {
    if (!hasInstance) {
      throw new IllegalStateException("There is no instance for manager to perform operation on.");
    }
  }

  /**
   * Helper method for determining if the given tableId exists in the instance.
   *
   * @param tableId The id of the table to check.
   * @throws IllegalStateException if the table does not exist in the instance.
   */
  private void checkHasTable(String tableId) {
    try (BigtableTableAdminClient tableAdminClient =
        bigtableResourceManagerClientFactory.bigtableTableAdminClient()) {
      if (!tableAdminClient.exists(tableId)) {
        throw new IllegalStateException(
            "The table " + tableId + " does not exist in instance " + instanceId + ".");
      }
    }
  }

  @Override
  public synchronized void createTable(String tableId, Iterable<String> columnFamilies) {
    createTable(tableId, columnFamilies, Duration.ofHours(1));
  }

  @Override
  public synchronized void createTable(
      String tableId, Iterable<String> columnFamilies, Duration maxAge) {
    // Check table ID
    checkValidTableId(tableId);

    // Check for at least one column family
    if (!columnFamilies.iterator().hasNext()) {
      throw new IllegalArgumentException(
          "There must be at least one column family specified when creating a table.");
    }

    // Create a default instance if this resource manager has not already created one
    if (!hasInstance) {
      createInstance(
          generateDefaultClusters(
              instanceId,
              DEFAULT_CLUSTER_ZONE,
              DEFAULT_CLUSTER_NUM_NODES,
              DEFAULT_CLUSTER_STORAGE_TYPE));
    }
    checkHasInstance();

    LOG.info("Creating table using tableId '{}'.", tableId);

    // Fetch the Bigtable Table client and create the table if it does not already exist in the
    // instance
    try (BigtableTableAdminClient tableAdminClient =
        bigtableResourceManagerClientFactory.bigtableTableAdminClient()) {
      if (!tableAdminClient.exists(tableId)) {
        CreateTableRequest createTableRequest = CreateTableRequest.of(tableId);
        for (String columnFamily : columnFamilies) {
          createTableRequest.addFamily(columnFamily, GCRules.GCRULES.maxAge(maxAge));
        }
        tableAdminClient.createTable(createTableRequest);

      } else {
        throw new IllegalStateException(
            "Table " + tableId + " already exists for instance " + instanceId + ".");
      }
    } catch (Exception e) {
      throw new BigtableResourceManagerException("Failed to create table.", e);
    }

    LOG.info("Successfully created table {}.{}", instanceId, tableId);
  }

  @Override
  public void write(RowMutation tableRow) {
    write(ImmutableList.of(tableRow));
  }

  @Override
  public synchronized void write(Iterable<RowMutation> tableRows) {
    checkHasInstance();

    // Exit early if there are no mutations
    if (!tableRows.iterator().hasNext()) {
      return;
    }

    LOG.info("Sending {} mutations to instance {}.", Iterables.size(tableRows), instanceId);

    // Fetch the Bigtable data client and send row mutations to the table
    try (BigtableDataClient dataClient =
        bigtableResourceManagerClientFactory.bigtableDataClient()) {
      for (RowMutation tableRow : tableRows) {
        dataClient.mutateRow(tableRow);
      }
    } catch (Exception e) {
      throw new BigtableResourceManagerException("Failed to write mutations.", e);
    }

    LOG.info("Successfully sent mutations to instance {}.", instanceId);
  }

  @Override
  public synchronized ImmutableList<Row> readTable(String tableId) {
    checkHasInstance();
    checkHasTable(tableId);

    // List to store fetched rows
    ImmutableList.Builder<Row> tableRowsBuilder = ImmutableList.builder();

    LOG.info("Reading all rows from {}.{}", instanceId, tableId);

    // Fetch the Bigtable data client and read all the rows from the table given by tableId
    try (BigtableDataClient dataClient =
        bigtableResourceManagerClientFactory.bigtableDataClient()) {

      Query query = Query.create(tableId);
      ServerStream<Row> rowStream = dataClient.readRows(query);
      for (Row row : rowStream) {
        tableRowsBuilder.add(row);
      }

    } catch (Exception e) {
      throw new BigtableResourceManagerException("Error occurred while reading table rows.", e);
    }

    ImmutableList<Row> tableRows = tableRowsBuilder.build();
    LOG.info("Loaded {} rows from {}.{}", tableRows.size(), instanceId, tableId);

    return tableRows;
  }

  @Override
  public synchronized void cleanupAll() {
    LOG.info("Attempting to cleanup manager.");
    try (BigtableInstanceAdminClient instanceAdminClient =
        bigtableResourceManagerClientFactory.bigtableInstanceAdminClient()) {
      instanceAdminClient.deleteInstance(instanceId);
      hasInstance = false;
    } catch (Exception e) {
      throw new BigtableResourceManagerException("Failed to delete resources.", e);
    }

    LOG.info("Manager successfully cleaned up.");
  }

  /** Builder for {@link DefaultBigtableResourceManager}. */
  public static final class Builder {

    private final String testId;
    private final String projectId;
    private CredentialsProvider credentialsProvider;

    private Builder(String testId, String projectId) {

      this.testId = testId;
      this.projectId = projectId;
    }

    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public DefaultBigtableResourceManager build() throws IOException {
      return new DefaultBigtableResourceManager(this);
    }
  }
}
