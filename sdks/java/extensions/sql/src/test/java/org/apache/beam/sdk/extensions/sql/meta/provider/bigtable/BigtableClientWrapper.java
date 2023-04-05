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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigtable;

import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.byteString;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.byteStringUtf8;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.io.IOException;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;

class BigtableClientWrapper implements Serializable {
  private final BigtableTableAdminClient tableAdminClient;
  private final BigtableDataClient dataClient;

  BigtableClientWrapper(
      String project,
      String instanceId,
      @Nullable Integer emulatorPort,
      @Nullable Credentials gcpCredentials)
      throws IOException {
    BigtableDataSettings.Builder settings =
        BigtableDataSettings.newBuilderForEmulator(emulatorPort)
            .setProjectId(project)
            .setInstanceId(instanceId)
            .setCredentialsProvider(FixedCredentialsProvider.create(gcpCredentials));

    settings
        .stubSettings()
        .setHeaderProvider(FixedHeaderProvider.create("user-agent", "apache-beam-test"));
    dataClient = BigtableDataClient.create(settings.build());
    BigtableTableAdminSettings tableSettings =
        BigtableTableAdminSettings.newBuilderForEmulator(emulatorPort)
            .setProjectId(project)
            .setInstanceId(instanceId)
            .build();
    tableAdminClient = BigtableTableAdminClient.create(tableSettings);
  }

  void writeRow(
      String key,
      String table,
      String familyColumn,
      String columnQualifier,
      byte[] value,
      long timestampMicros) {
    RowMutation rowMutation =
        RowMutation.create(table, key)
            .setCell(
                familyColumn, byteStringUtf8(columnQualifier), timestampMicros, byteString(value));
    dataClient.mutateRow(rowMutation);
  }

  void createTable(String tableName, String familyName) {
    CreateTableRequest createTableRequest = CreateTableRequest.of(tableName).addFamily(familyName);
    tableAdminClient.createTable(createTableRequest);
  }

  void deleteTable(String tableId) {
    tableAdminClient.deleteTable(tableId);
  }

  void closeSession() throws IOException {
    dataClient.close();
    tableAdminClient.close();
  }
}
