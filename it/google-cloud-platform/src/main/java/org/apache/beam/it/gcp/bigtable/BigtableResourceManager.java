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
package org.apache.beam.it.gcp.bigtable;

import static org.apache.beam.it.common.utils.ResourceManagerUtils.checkValidProjectId;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.checkValidTableId;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateDefaultClusters;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateInstanceId;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.oauth2.GoogleCredentials;
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
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.it.common.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * Client for managing Bigtable resources.
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
public class BigtableResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableResourceManager.class);
  private static final String DEFAULT_CLUSTER_ZONE = "us-central1-a";
  private static final int DEFAULT_CLUSTER_NUM_NODES = 1;
  private static final StorageType DEFAULT_CLUSTER_STORAGE_TYPE = StorageType.SSD;

  private final String projectId;
  private final String instanceId;
  private final BigtableResourceManagerClientFactory bigtableResourceManagerClientFactory;

  // List to store created tables for static RM
  private List<String> createdTables;

  private boolean hasInstance;
  private final boolean usingStaticInstance;

  private BigtableResourceManager(Builder builder) throws IOException {
    this(builder, null);
  }

  @VisibleForTesting
  BigtableResourceManager(
      Builder builder,
      @Nullable BigtableResourceManagerClientFactory bigtableResourceManagerClientFactory)
      throws IOException {

    // Check that the project ID conforms to GCP standards
    checkValidProjectId(builder.projectId);
    this.projectId = builder.projectId;
    this.createdTables = new ArrayList<>();

    // Check if RM was configured to use static Bigtable instance.
    if (builder.useStaticInstance) {
      if (builder.instanceId == null) {
        throw new BigtableResourceManagerException(
            "This manager was configured to use a static resource, but the instanceId was not properly set.");
      }
      this.instanceId = builder.instanceId;
      this.hasInstance = true;
    } else {
      if (builder.instanceId != null) {
        throw new BigtableResourceManagerException(
            "The instanceId property was set in the builder, but the useStaticInstance() method was not called.");
      }
      // Generate instance id based on given test id.
      this.instanceId = generateInstanceId(builder.testId);
      this.hasInstance = false;
    }
    this.usingStaticInstance = builder.useStaticInstance;

    if (bigtableResourceManagerClientFactory != null) {
      this.bigtableResourceManagerClientFactory = bigtableResourceManagerClientFactory;

    } else {
      // Create the bigtable admin and data client settings builders, and set the necessary id's for
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

      this.bigtableResourceManagerClientFactory =
          new BigtableResourceManagerClientFactory(
              bigtableInstanceAdminSettings.build(),
              bigtableTableAdminSettings.build(),
              bigtableDataSettings.build());
    }
  }

  public static Builder builder(String testId, String projectId) throws IOException {
    return new Builder(testId, projectId);
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

  /**
   * Creates a Bigtable instance in which all clusters, nodes and tables will exist.
   *
   * @param clusters Collection of BigtableResourceManagerCluster objects to associate with the
   *     given Bigtable instance.
   * @throws BigtableResourceManagerException if there is an error creating the instance in
   *     Bigtable.
   */
  public synchronized void createInstance(Iterable<BigtableResourceManagerCluster> clusters)
      throws BigtableResourceManagerException {

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

  /**
   * Creates a table within the current instance given a table ID and a collection of column family
   * names.
   *
   * <p>Bigtable has the capability to store multiple versions of data in a single cell, which are
   * indexed using timestamp values. The timestamp can be set by Bigtable, with the default
   * timestamp value being 1970-01-01, or can be set explicitly. The columns in the created table
   * will be automatically garbage collected once they reach an age of 1-hour after the set
   * timestamp.
   *
   * <p>Note: Implementations may do instance creation here, if one does not already exist.
   *
   * @param tableId The id of the table.
   * @param columnFamilies A collection of column family names for the table.
   * @throws BigtableResourceManagerException if there is an error creating the table in Bigtable.
   */
  public synchronized void createTable(String tableId, Iterable<String> columnFamilies)
      throws BigtableResourceManagerException {
    createTable(tableId, columnFamilies, Duration.ofHours(1));
  }

  /**
   * Creates a table within the current instance given a table ID and a collection of column family
   * names.
   *
   * <p>Bigtable has the capability to store multiple versions of data in a single cell, which are
   * indexed using timestamp values. The timestamp can be set by Bigtable, with the default
   * timestamp value being 1970-01-01, or can be set explicitly. The columns in the created table
   * will be automatically garbage collected once they reach an age of {@code maxAge} after the set
   * timestamp.
   *
   * <p>Note: Implementations may do instance creation here, if one does not already exist.
   *
   * @param tableId The id of the table.
   * @param columnFamilies A collection of column family names for the table.
   * @param maxAge Sets the maximum age the columns can persist before being garbage collected.
   * @throws BigtableResourceManagerException if there is an error creating the table in Bigtable.
   */
  public synchronized void createTable(
      String tableId, Iterable<String> columnFamilies, Duration maxAge)
      throws BigtableResourceManagerException {
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

    if (usingStaticInstance) {
      createdTables.add(tableId);
    }

    LOG.info("Successfully created table {}.{}", instanceId, tableId);
  }

  /**
   * Writes a given row into a table. This method requires {@link
   * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table
   * beforehand.
   *
   * @param tableRow A mutation object representing the table row.
   * @throws BigtableResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no instance, if the table does not exist or if there is an
   *     IOException when attempting to retrieve the bigtable data client.
   */
  public void write(RowMutation tableRow) throws BigtableResourceManagerException {
    write(ImmutableList.of(tableRow));
  }

  /**
   * Writes a collection of table rows into one or more tables. This method requires {@link
   * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table
   * beforehand.
   *
   * @param tableRows A collection of mutation objects representing table rows.
   * @throws BigtableResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no instance, if the table does not exist or if there is an
   *     IOException when attempting to retrieve the bigtable data client.
   */
  public synchronized void write(Iterable<RowMutation> tableRows)
      throws BigtableResourceManagerException {
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

  /**
   * Reads all the rows in a table. This method requires {@link
   * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table
   * beforehand.
   *
   * @param tableId The id of table to read rows from.
   * @return A List object containing all the rows in the table.
   * @throws BigtableResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no instance, if the table does not exist or if there is an
   *     IOException when attempting to retrieve the bigtable data client.
   */
  public synchronized ImmutableList<Row> readTable(String tableId)
      throws BigtableResourceManagerException {
    return readTable(tableId, null);
  }

  /**
   * Reads all the rows in a table. This method requires {@link
   * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table
   * beforehand.
   *
   * @param tableId The id of table to read rows from.
   * @param limit Limits the number of rows that can be returned
   * @return A List object containing all the rows in the table.
   * @throws BigtableResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no instance, if the table does not exist or if there is an
   *     IOException when attempting to retrieve the bigtable data client.
   */
  public synchronized ImmutableList<Row> readTable(String tableId, @Nullable Long limit)
      throws BigtableResourceManagerException {
    checkHasInstance();
    checkHasTable(tableId);

    // List to store fetched rows
    ImmutableList.Builder<Row> tableRowsBuilder = ImmutableList.builder();

    LOG.info("Reading all rows from {}.{}", instanceId, tableId);

    // Fetch the Bigtable data client and read all the rows from the table given by tableId
    try (BigtableDataClient dataClient =
        bigtableResourceManagerClientFactory.bigtableDataClient()) {

      Query query = Query.create(tableId);
      if (limit != null) {
        query.limit(limit);
      }
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

  /**
   * Deletes all created resources (instance and tables) and cleans up all Bigtable clients, making
   * the manager object unusable.
   *
   * <p>If this Resource Manager was configured to use a static instance, the instance will not be
   * cleaned up, but any created tables will be deleted.
   *
   * @throws BigtableResourceManagerException if there is an error deleting the instance or tables
   *     in Bigtable.
   */
  @Override
  public synchronized void cleanupAll() throws BigtableResourceManagerException {
    LOG.info("Attempting to cleanup manager.");

    if (usingStaticInstance) {
      try (BigtableTableAdminClient tableAdminClient =
          bigtableResourceManagerClientFactory.bigtableTableAdminClient()) {
        createdTables.forEach(tableAdminClient::deleteTable);
        LOG.info(
            "This manager was configured to use a static instance that will not be cleaned up.");
        return;
      }
    }

    if (hasInstance) {
      try (BigtableInstanceAdminClient instanceAdminClient =
          bigtableResourceManagerClientFactory.bigtableInstanceAdminClient()) {
        instanceAdminClient.deleteInstance(instanceId);
        hasInstance = false;
      } catch (Exception e) {
        throw new BigtableResourceManagerException("Failed to delete resources.", e);
      }
    }
    LOG.info("Manager successfully cleaned up.");
  }

  /** Builder for {@link BigtableResourceManager}. */
  public static final class Builder {

    private final String testId;
    private final String projectId;
    private @Nullable String instanceId;
    private boolean useStaticInstance;
    private CredentialsProvider credentialsProvider;

    private Builder(String testId, String projectId) throws IOException {
      this.testId = testId;
      this.projectId = projectId;
      this.credentialsProvider =
          FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault());
      this.instanceId = null;
    }

    /**
     * Set the GCP credentials provider to connect to the project defined in the builder.
     *
     * @param credentialsProvider The GCP CredentialsProvider.
     * @return this builder with the CredentialsProvider set.
     */
    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    /**
     * Set the instance ID of a static Bigtable instance for this Resource Manager to manage.
     *
     * @return this builder with the instance ID set.
     */
    public Builder setInstanceId(String instanceId) {
      this.instanceId = instanceId;
      return this;
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
      if (System.getProperty("bigtableInstanceId") != null) {
        this.useStaticInstance = true;
        this.instanceId = System.getProperty("bigtableInstanceId");
      }
      return this;
    }

    public BigtableResourceManager build() throws IOException {
      return new BigtableResourceManager(this);
    }
  }
}
