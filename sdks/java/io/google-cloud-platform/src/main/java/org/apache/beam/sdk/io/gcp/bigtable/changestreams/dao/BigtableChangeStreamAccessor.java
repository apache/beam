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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableConfig;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.threeten.bp.Duration;

/**
 * This is probably a temporary solution to what is a bigger migration from
 * cloud-bigtable-client-core to java-bigtable.
 *
 * <p>This class creates and maintains the lifecycle java-bigtable clients to interact with Cloud
 * Bigtable. This class creates singletons of data and admin clients for each
 * project/instance/table/app profile. Per workers, there should only be 1 instance of the client
 * for each table/app profile. This ensures we're not creating many/excessive connections with the
 * backend and the jobs on the same machine shares the same sets of connections.
 */
@Internal
class BigtableChangeStreamAccessor {
  // Create one bigtable data/admin client per bigtable config (project/instance/table/app profile)
  private static final ConcurrentHashMap<BigtableConfig, BigtableChangeStreamAccessor>
      bigtableAccessors = new ConcurrentHashMap<>();

  private final BigtableDataClient dataClient;
  private final BigtableTableAdminClient tableAdminClient;
  private final BigtableInstanceAdminClient instanceAdminClient;

  private BigtableChangeStreamAccessor(
      BigtableDataClient dataClient,
      BigtableTableAdminClient tableAdminClient,
      BigtableInstanceAdminClient instanceAdminClient) {
    this.dataClient = dataClient;
    this.tableAdminClient = tableAdminClient;
    this.instanceAdminClient = instanceAdminClient;
  }

  /**
   * Create a BigtableAccess if it doesn't exist and store it in the cache for faster access. If it
   * does exist, just return it.
   *
   * @param bigtableConfig config that contains all the parameters to connect to a Cloud Bigtable
   *     instance
   * @return data and admin clients connected to the Cloud Bigtable instance
   * @throws IOException if the connection fails
   */
  public static synchronized BigtableChangeStreamAccessor getOrCreate(
      @NonNull BigtableConfig bigtableConfig) throws IOException {
    if (bigtableAccessors.get(bigtableConfig) == null) {
      BigtableChangeStreamAccessor bigtableAccessor =
          BigtableChangeStreamAccessor.createAccessor(bigtableConfig);
      bigtableAccessors.put(bigtableConfig, bigtableAccessor);
    }
    return checkStateNotNull(bigtableAccessors.get(bigtableConfig));
  }

  private static BigtableChangeStreamAccessor createAccessor(@NonNull BigtableConfig bigtableConfig)
      throws IOException {
    String projectId = checkArgumentNotNull(bigtableConfig.getProjectId()).get();
    String instanceId = checkArgumentNotNull(bigtableConfig.getInstanceId()).get();
    String appProfileId = checkArgumentNotNull(bigtableConfig.getAppProfileId()).get();
    BigtableDataSettings.Builder dataSettingsBuilder = BigtableDataSettings.newBuilder();
    BigtableTableAdminSettings.Builder tableAdminSettingsBuilder =
        BigtableTableAdminSettings.newBuilder();
    BigtableInstanceAdminSettings.Builder instanceAdminSettingsBuilder =
        BigtableInstanceAdminSettings.newBuilder();

    dataSettingsBuilder.setProjectId(projectId);
    tableAdminSettingsBuilder.setProjectId(projectId);
    instanceAdminSettingsBuilder.setProjectId(projectId);

    dataSettingsBuilder.setInstanceId(instanceId);
    tableAdminSettingsBuilder.setInstanceId(instanceId);

    if (appProfileId != null) {
      dataSettingsBuilder.setAppProfileId(appProfileId);
    }

    RetrySettings.Builder readRowRetrySettings =
        dataSettingsBuilder.stubSettings().readRowSettings().retrySettings();
    dataSettingsBuilder
        .stubSettings()
        .readRowSettings()
        .setRetrySettings(
            readRowRetrySettings
                .setInitialRpcTimeout(Duration.ofSeconds(30))
                .setTotalTimeout(Duration.ofSeconds(30))
                .setMaxRpcTimeout(Duration.ofSeconds(30))
                .setMaxAttempts(10)
                .build());

    RetrySettings.Builder readRowsRetrySettings =
        dataSettingsBuilder.stubSettings().readRowsSettings().retrySettings();
    dataSettingsBuilder
        .stubSettings()
        .readRowsSettings()
        .setRetrySettings(
            readRowsRetrySettings
                .setInitialRpcTimeout(Duration.ofSeconds(30))
                .setTotalTimeout(Duration.ofSeconds(30))
                .setMaxRpcTimeout(Duration.ofSeconds(30))
                .setMaxAttempts(10)
                .build());

    RetrySettings.Builder mutateRowRetrySettings =
        dataSettingsBuilder.stubSettings().mutateRowSettings().retrySettings();
    dataSettingsBuilder
        .stubSettings()
        .mutateRowSettings()
        .setRetrySettings(
            mutateRowRetrySettings
                .setInitialRpcTimeout(Duration.ofSeconds(30))
                .setTotalTimeout(Duration.ofSeconds(30))
                .setMaxRpcTimeout(Duration.ofSeconds(30))
                .setMaxAttempts(10)
                .build());

    RetrySettings.Builder checkAndMutateRowRetrySettings =
        dataSettingsBuilder.stubSettings().checkAndMutateRowSettings().retrySettings();
    dataSettingsBuilder
        .stubSettings()
        .checkAndMutateRowSettings()
        .setRetrySettings(
            checkAndMutateRowRetrySettings
                .setInitialRpcTimeout(Duration.ofSeconds(30))
                .setTotalTimeout(Duration.ofSeconds(30))
                .setMaxRpcTimeout(Duration.ofSeconds(30))
                .setMaxAttempts(10)
                .build());

    RetrySettings.Builder readChangeStreamRetrySettings =
        dataSettingsBuilder.stubSettings().readChangeStreamSettings().retrySettings();
    dataSettingsBuilder
        .stubSettings()
        .readChangeStreamSettings()
        .setRetrySettings(
            readChangeStreamRetrySettings
                // Set timeouts to 60s - dataflow should checkpoint before then, but it is not
                // guaranteed to happen after a specific duration. We still want a conservative
                // timeout so that it can't hang.
                .setInitialRpcTimeout(Duration.ofSeconds(60))
                .setTotalTimeout(Duration.ofSeconds(60))
                .setMaxRpcTimeout(Duration.ofSeconds(60))
                .setMaxAttempts(3)
                .build());

    BigtableDataClient dataClient = BigtableDataClient.create(dataSettingsBuilder.build());
    BigtableTableAdminClient tableAdminClient =
        BigtableTableAdminClient.create(tableAdminSettingsBuilder.build());
    BigtableInstanceAdminClient instanceAdminClient =
        BigtableInstanceAdminClient.create(instanceAdminSettingsBuilder.build());
    return new BigtableChangeStreamAccessor(dataClient, tableAdminClient, instanceAdminClient);
  }

  public BigtableDataClient getDataClient() {
    return dataClient;
  }

  public BigtableTableAdminClient getTableAdminClient() {
    return tableAdminClient;
  }

  public BigtableInstanceAdminClient getInstanceAdminClient() {
    return instanceAdminClient;
  }
}
