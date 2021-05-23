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
package org.apache.beam.sdk.io.gcp.spanner.cdc;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class PipelineInitializer {

  public void initialize(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createMetadataTable(dbAdminClient, id);
  }

  private void createMetadataTable(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    final String metadataCreateStmt =
        "CREATE TABLE CDC_Partitions_"
            + id.getName()
            + "_"
            + UUID.randomUUID()
            + " ("
            + "  PartitionToken STRING(MAX) NOT NULL,"
            + "  ParentToken STRING(MAX) NOT NULL,"
            + "  StartTimestamp TIMESTAMP NOT NULL,"
            + "  InclusiveStart BOOL NOT NULL, "
            + "  EndTimestamp TIMESTAMP,"
            + "  InclusiveEnd BOOL,"
            + "  HeartbeatSeconds INT64 NOT NULL,"
            + "  State STRING(MAX) NOT NULL,"
            + "  CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),"
            + "  UpdatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)"
            + ") PRIMARY KEY (PartitionToken);";
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        dbAdminClient.updateDatabaseDdl(
            id.getInstanceId().getInstance(),
            id.getDatabase(),
            Collections.singletonList(metadataCreateStmt),
            null);
    try {
      // Initiate the request which returns an OperationFuture.
      op.get();
    } catch (ExecutionException e) {
      // If the operation failed during execution, expose the cause.
      throw (SpannerException) e.getCause();
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }
}
