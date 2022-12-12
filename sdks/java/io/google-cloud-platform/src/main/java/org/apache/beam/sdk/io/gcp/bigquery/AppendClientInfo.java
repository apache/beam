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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Container class used by {@link StorageApiWritesShardedRecords} and {@link
 * StorageApiWritesShardedRecords} to enapsulate a destination {@link TableSchema} along with a
 * {@link BigQueryServices.StreamAppendClient} and other objects needed to write records.
 */
class AppendClientInfo {
  @Nullable BigQueryServices.StreamAppendClient streamAppendClient;
  @Nullable TableSchema tableSchema;
  Consumer<BigQueryServices.StreamAppendClient> closeAppendClient;
  Descriptors.Descriptor descriptor;

  public AppendClientInfo(
      TableSchema tableSchema, Consumer<BigQueryServices.StreamAppendClient> closeAppendClient)
      throws Exception {
    this.tableSchema = tableSchema;
    this.descriptor = TableRowToStorageApiProto.getDescriptorFromTableSchema(tableSchema, true);
    this.closeAppendClient = closeAppendClient;
  }

  public AppendClientInfo createAppendClient(
      BigQueryServices.DatasetService datasetService,
      Supplier<String> getStreamName,
      boolean useConnectionPool)
      throws Exception {
    if (streamAppendClient == null) {
      this.streamAppendClient =
          datasetService.getStreamAppendClient(getStreamName.get(), descriptor, useConnectionPool);
    }
    return this;
  }

  public void close() {
    if (streamAppendClient != null) {
      closeAppendClient.accept(streamAppendClient);
    }
  }
}
