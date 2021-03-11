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

import com.google.auth.Credentials;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import java.io.IOException;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;

class BigtableClientWrapper implements Serializable {
  private final BigtableTableAdminClient tableAdminClient;
  private final BigtableDataClient dataClient;
  private final BigtableSession session;
  private final BigtableOptions bigtableOptions;

  BigtableClientWrapper(
      String project,
      String instanceId,
      @Nullable Integer emulatorPort,
      @Nullable Credentials gcpCredentials)
      throws IOException {
    BigtableOptions.Builder optionsBuilder =
        BigtableOptions.builder()
            .setProjectId(project)
            .setInstanceId(instanceId)
            .setUserAgent("apache-beam-test");
    if (emulatorPort != null) {
      optionsBuilder.enableEmulator("localhost", emulatorPort);
    }
    if (gcpCredentials != null) {
      optionsBuilder.setCredentialOptions(CredentialOptions.credential(gcpCredentials));
    }
    bigtableOptions = optionsBuilder.build();

    session = new BigtableSession(bigtableOptions);
    tableAdminClient = session.getTableAdminClient();
    dataClient = session.getDataClient();
  }

  void writeRow(
      String key,
      String table,
      String familyColumn,
      String columnQualifier,
      byte[] value,
      long timestampMicros) {
    Mutation.SetCell setCell =
        Mutation.SetCell.newBuilder()
            .setFamilyName(familyColumn)
            .setColumnQualifier(byteStringUtf8(columnQualifier))
            .setValue(byteString(value))
            .setTimestampMicros(timestampMicros)
            .build();
    Mutation mutation = Mutation.newBuilder().setSetCell(setCell).build();
    MutateRowRequest mutateRowRequest =
        MutateRowRequest.newBuilder()
            .setRowKey(byteStringUtf8(key))
            .setTableName(bigtableOptions.getInstanceName().toTableNameStr(table))
            .addMutations(mutation)
            .build();
    dataClient.mutateRow(mutateRowRequest);
  }

  void createTable(String tableName, String familyName) {
    Table.Builder tableBuilder = Table.newBuilder();
    tableBuilder.putColumnFamilies(familyName, ColumnFamily.newBuilder().build());

    String instanceName = bigtableOptions.getInstanceName().toString();
    com.google.bigtable.admin.v2.CreateTableRequest.Builder createTableRequestBuilder =
        com.google.bigtable.admin.v2.CreateTableRequest.newBuilder()
            .setParent(instanceName)
            .setTableId(tableName)
            .setTable(tableBuilder.build());
    tableAdminClient.createTable(createTableRequestBuilder.build());
  }

  void deleteTable(String tableId) {
    final String tableName = bigtableOptions.getInstanceName().toTableNameStr(tableId);
    DeleteTableRequest.Builder deleteTableRequestBuilder =
        DeleteTableRequest.newBuilder().setName(tableName);
    tableAdminClient.deleteTable(deleteTableRequestBuilder.build());
  }

  void closeSession() throws IOException {
    session.close();
  }
}
