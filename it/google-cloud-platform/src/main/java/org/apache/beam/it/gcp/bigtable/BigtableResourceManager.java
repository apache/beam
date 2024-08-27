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
import static org.awaitility.Awaitility.await;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.AppProfile;
import com.google.cloud.bigtable.admin.v2.models.AppProfile.MultiClusterRoutingPolicy;
import com.google.cloud.bigtable.admin.v2.models.AppProfile.RoutingPolicy;
import com.google.cloud.bigtable.admin.v2.models.AppProfile.SingleClusterRoutingPolicy;
import com.google.cloud.bigtable.admin.v2.models.Cluster;
import com.google.cloud.bigtable.admin.v2.models.CreateAppProfileRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateInstanceRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.admin.v2.models.UpdateTableRequest;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.it.common.ResourceManager;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
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
  private static final String DEFAULT_CLUSTER_ZONE = "us-central1-b";
  private static final int DEFAULT_CLUSTER_NUM_NODES = 10;
  private static final StorageType DEFAULT_CLUSTER_STORAGE_TYPE = StorageType.SSD;

  private final String projectId;
  private final String instanceId;
  private final BigtableResourceManagerClientFactory bigtableResourceManagerClientFactory;

  // List to store created tables for static RM
  private final List<String> createdTables;
  // List to store created app profiles for static RM
  private final List<String> createdAppProfiles;
  // List of tables we enabled CDC for
  private final Set<String> cdcEnabledTables;

  private boolean hasInstance;
  private List<BigtableResourceManagerCluster> clusters;

  private final boolean usingStaticInstance;

  private BigtableResourceManager(Builder builder) throws IOException {
    this(builder, null);
  }

  @VisibleForTesting
  BigtableResourceManager(
      Builder builder,
      @Nullable BigtableResourceManagerClientFactory bigtableResourceManagerClientFactory)
      throws BigtableResourceManagerException, IOException {

    // Check that the project ID conforms to GCP standards
    checkValidProjectId(builder.projectId);
    this.projectId = builder.projectId;
    this.createdTables = new ArrayList<>();
    this.createdAppProfiles = new ArrayList<>();
    this.cdcEnabledTables = new HashSet<>();
    this.clusters = new ArrayList<>();

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

  public static Builder builder(
      String testId, String projectId, CredentialsProvider credentialsProvider) {
    return new Builder(testId, projectId, credentialsProvider);
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
   * @param clusters List of BigtableResourceManagerCluster objects to associate with the given
   *     Bigtable instance.
   * @throws BigtableResourceManagerException if there is an error creating the instance in
   *     Bigtable.
   */
  public synchronized void createInstance(List<BigtableResourceManagerCluster> clusters)
      throws BigtableResourceManagerException {

    // Check to see if instance already exists, and throw error if it does
    if (hasInstance) {
      LOG.warn(
          "Skipping instance creation. Instance was already created or static instance was passed. Reusing : {}.",
          instanceId);
      return;
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
    this.clusters = clusters;

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
    BigtableTableSpec spec = new BigtableTableSpec();
    spec.setColumnFamilies(columnFamilies);
    spec.setMaxAge(maxAge);
    createTable(tableId, spec);
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
   * @param bigtableTableSpec Other table configurations
   * @throws BigtableResourceManagerException if there is an error creating the table in Bigtable.
   */
  public synchronized void createTable(String tableId, BigtableTableSpec bigtableTableSpec)
      throws BigtableResourceManagerException {
    // Check table ID
    checkValidTableId(tableId);

    // Check for at least one column family
    if (!bigtableTableSpec.getColumnFamilies().iterator().hasNext()) {
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
        for (String columnFamily : bigtableTableSpec.getColumnFamilies()) {
          createTableRequest.addFamily(
              columnFamily, GCRules.GCRULES.maxAge(bigtableTableSpec.getMaxAge()));
        }
        if (bigtableTableSpec.getCdcEnabled()) {
          createTableRequest.addChangeStreamRetention(Duration.ofDays(7));
          cdcEnabledTables.add(tableId);
        }
        tableAdminClient.createTable(createTableRequest);

        await("Waiting for all tables to be replicated.")
            .atMost(java.time.Duration.ofMinutes(10))
            .pollInterval(java.time.Duration.ofSeconds(5))
            .until(
                () -> {
                  Table t = tableAdminClient.getTable(tableId);
                  Map<String, Table.ReplicationState> rs = t.getReplicationStatesByClusterId();
                  return rs.values().stream().allMatch(Table.ReplicationState.READY::equals);
                });

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
   * Creates an application profile within the current instance
   *
   * <p>Note: Implementations may do instance creation here, if one does not already exist.
   *
   * @param appProfileId The id of the app profile.
   * @param allowTransactionWrites Allows transactional writes when single cluster routing is
   *     enabled
   * @param clusters Clusters where traffic is going to be routed. If more than one cluster is
   *     specified, a multi-cluster routing is used. A single-cluster routing is used when a single
   *     cluster is specified.
   * @throws BigtableResourceManagerException if there is an error creating the application profile
   *     in Bigtable.
   */
  public synchronized void createAppProfile(
      String appProfileId, boolean allowTransactionWrites, List<String> clusters)
      throws BigtableResourceManagerException {
    checkHasInstance();
    if (clusters == null || clusters.isEmpty()) {
      throw new IllegalArgumentException("Cluster list cannot be empty");
    }

    RoutingPolicy routingPolicy;

    if (clusters.size() == 1) {
      routingPolicy = SingleClusterRoutingPolicy.of(clusters.get(0), allowTransactionWrites);
    } else {
      routingPolicy = MultiClusterRoutingPolicy.of(new HashSet<>(clusters));
    }

    LOG.info("Creating appProfile {} for instance project {}.", appProfileId, instanceId);

    try (BigtableInstanceAdminClient instanceAdminClient =
        bigtableResourceManagerClientFactory.bigtableInstanceAdminClient()) {
      List<AppProfile> existingAppProfiles = instanceAdminClient.listAppProfiles(instanceId);
      if (!doesAppProfileExist(appProfileId, existingAppProfiles)) {
        CreateAppProfileRequest request =
            CreateAppProfileRequest.of(instanceId, appProfileId).setRoutingPolicy(routingPolicy);
        instanceAdminClient.createAppProfile(request);
        LOG.info("Successfully created appProfile {}.", appProfileId);
      } else {
        throw new IllegalStateException(
            "App profile " + appProfileId + " already exists for instance " + instanceId + ".");
      }
    } catch (Exception e) {
      throw new BigtableResourceManagerException("Failed to create app profile.", e);
    }

    if (usingStaticInstance) {
      createdAppProfiles.add(appProfileId);
    }
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

  /** Get all the cluster names of the current instance. */
  public List<String> getClusterNames() {
    return StreamSupport.stream(getClusters().spliterator(), false)
        .map(BigtableResourceManagerCluster::clusterId)
        .collect(Collectors.toList());
  }

  private Iterable<BigtableResourceManagerCluster> getClusters() {
    if (usingStaticInstance && this.clusters.isEmpty()) {
      try (BigtableInstanceAdminClient instanceAdminClient =
          bigtableResourceManagerClientFactory.bigtableInstanceAdminClient()) {
        List<BigtableResourceManagerCluster> managedClusters = new ArrayList<>();
        for (Cluster cluster : instanceAdminClient.listClusters(instanceId)) {
          managedClusters.add(
              BigtableResourceManagerCluster.create(
                  cluster.getId(),
                  cluster.getZone(),
                  cluster.getServeNodes(),
                  cluster.getStorageType()));
        }
        this.clusters = managedClusters;
      }
    }
    return this.clusters;
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

    try (BigtableTableAdminClient tableAdminClient =
        bigtableResourceManagerClientFactory.bigtableTableAdminClient()) {
      // Change streams must be disabled before table or instance can be deleted
      for (String tableId : cdcEnabledTables) {
        tableAdminClient.updateTable(UpdateTableRequest.of(tableId).disableChangeStreamRetention());
      }

      if (usingStaticInstance) {
        LOG.info(
            "This manager was configured to use a static instance that will not be cleaned up.");

        // Remove managed tables
        createdTables.forEach(tableAdminClient::deleteTable);

        // Remove managed app profiles
        try (BigtableInstanceAdminClient instanceAdminClient =
            bigtableResourceManagerClientFactory.bigtableInstanceAdminClient()) {
          createdAppProfiles.forEach(
              profile -> instanceAdminClient.deleteAppProfile(instanceId, profile, true));
        }
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

  private boolean doesAppProfileExist(String appProfileId, List<AppProfile> existingAppProfiles) {
    for (AppProfile existingProfile : existingAppProfiles) {
      if (StringUtils.equals(appProfileId, existingProfile.getId())) {
        return true;
      }
    }
    return false;
  }

  /** Builder for {@link BigtableResourceManager}. */
  public static final class Builder {

    private final String testId;
    private final String projectId;
    private @Nullable String instanceId;
    private boolean useStaticInstance;
    private CredentialsProvider credentialsProvider;

    private Builder(String testId, String projectId, CredentialsProvider credentialsProvider) {
      this.testId = testId;
      this.projectId = projectId;
      this.credentialsProvider = credentialsProvider;
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
