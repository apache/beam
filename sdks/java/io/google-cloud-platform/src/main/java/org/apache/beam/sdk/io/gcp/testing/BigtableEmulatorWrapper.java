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
package org.apache.beam.sdk.io.gcp.testing;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public class BigtableEmulatorWrapper {
  private final BigtableTableAdminClient tableAdminClient;
  private final BigtableDataClient dataClient;

  public BigtableEmulatorWrapper(int emulatorPort, String projectId, String instanceId)
      throws IOException {
    BigtableTableAdminSettings.Builder tableAdminSettings =
        BigtableTableAdminSettings.newBuilderForEmulator(emulatorPort)
            .setProjectId(projectId)
            .setInstanceId(instanceId);
    tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings.build());

    BigtableDataSettings.Builder dataSettings =
        BigtableDataSettings.newBuilderForEmulator(emulatorPort)
            .setProjectId(projectId)
            .setInstanceId(instanceId);
    dataClient = BigtableDataClient.create(dataSettings.build());
  }

  public void writeRow(
      String key,
      String table,
      String familyColumn,
      String columnQualifier,
      byte[] value,
      long timestampMicros)
      throws ExecutionException, InterruptedException {
    ApiFuture<Void> mutateFuture =
        dataClient.mutateRowAsync(
            RowMutation.create(table, key)
                .setCell(
                    familyColumn,
                    ByteString.copyFromUtf8(columnQualifier),
                    timestampMicros,
                    ByteString.copyFrom(value)));
    mutateFuture.get();
  }

  public void createTable(String tableName, String... families) {
    CreateTableRequest request = CreateTableRequest.of(tableName);
    ImmutableList.copyOf(families).forEach(request::addFamily);
    tableAdminClient.createTable(request);
  }
}
