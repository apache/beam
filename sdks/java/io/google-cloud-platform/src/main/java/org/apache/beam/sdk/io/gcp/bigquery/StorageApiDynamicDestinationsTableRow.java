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

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

public class StorageApiDynamicDestinationsTableRow<T, DestinationT extends @NonNull Object>
    extends StorageApiDynamicDestinations<T, DestinationT> {
  private final SerializableFunction<T, TableRow> formatFunction;
  private final CreateDisposition createDisposition;
  private final boolean ignoreUnknownValues;
  private static final TableSchemaCache SCHEMA_CACHE =
      new TableSchemaCache(Duration.standardSeconds(1));

  static {
    SCHEMA_CACHE.start();
  }

  StorageApiDynamicDestinationsTableRow(
      DynamicDestinations<T, DestinationT> inner,
      SerializableFunction<T, TableRow> formatFunction,
      CreateDisposition createDisposition,
      boolean ignoreUnknownValues) {
    super(inner);
    this.formatFunction = formatFunction;
    this.createDisposition = createDisposition;
    this.ignoreUnknownValues = ignoreUnknownValues;
  }

  static void clearSchemaCache() throws ExecutionException, InterruptedException {
    SCHEMA_CACHE.clear();
  }

  @Override
  public MessageConverter<T> getMessageConverter(
      DestinationT destination, DatasetService datasetService) throws Exception {
    return new TableRowConverter(destination, datasetService);
  }

  class TableRowConverter implements MessageConverter<T> {
    final @Nullable TableSchema tableSchema;
    final com.google.cloud.bigquery.storage.v1.TableSchema protoTableSchema;
    final TableRowToStorageApiProto.SchemaInformation schemaInformation;
    final Descriptor descriptor;

    TableRowConverter(
        TableSchema tableSchema,
        TableRowToStorageApiProto.SchemaInformation schemaInformation,
        Descriptor descriptor) {
      this.tableSchema = tableSchema;
      this.protoTableSchema = TableRowToStorageApiProto.schemaToProtoTableSchema(tableSchema);
      this.schemaInformation = schemaInformation;
      this.descriptor = descriptor;
    }

    TableRowConverter(DestinationT destination, DatasetService datasetService) throws Exception {
      TableSchema localTableSchema = getSchema(destination);
      TableReference tableReference = getTable(destination).getTableReference();
      if (localTableSchema == null) {
        // If the table already exists, then try and fetch the schema from the existing
        // table.
        localTableSchema = SCHEMA_CACHE.getSchema(tableReference, datasetService);
        if (localTableSchema == null) {
          if (createDisposition == CreateDisposition.CREATE_NEVER) {
            throw new RuntimeException(
                "BigQuery table "
                    + tableReference
                    + " not found. If you wanted to "
                    + "automatically create the table, set the create disposition to CREATE_IF_NEEDED and specify a "
                    + "schema.");
          } else {
            throw new RuntimeException(
                "Schema must be set for table "
                    + tableReference
                    + " when writing TableRows using Storage API and "
                    + "using a create disposition of CREATE_IF_NEEDED.");
          }
        }
      } else {
        // Make sure we register this schema with the cache, unless there's already a more
        // up-to-date schema.
        localTableSchema =
            MoreObjects.firstNonNull(
                SCHEMA_CACHE.putSchemaIfAbsent(tableReference, localTableSchema), localTableSchema);
      }
      this.tableSchema = localTableSchema;
      this.protoTableSchema = TableRowToStorageApiProto.schemaToProtoTableSchema(tableSchema);
      schemaInformation =
          TableRowToStorageApiProto.SchemaInformation.fromTableSchema(protoTableSchema);
      descriptor =
          TableRowToStorageApiProto.getDescriptorFromTableSchema(
              Preconditions.checkStateNotNull(tableSchema), true);
    }

    @Override
    public com.google.cloud.bigquery.storage.v1.TableSchema getTableSchema() {
      return protoTableSchema;
    }

    @Override
    public TableRow toTableRow(T element) {
      return formatFunction.apply(element);
    }

    @Override
    public StorageApiWritePayload toMessage(T element) throws Exception {
      return toMessage(formatFunction.apply(element), true);
    }

    public StorageApiWritePayload toMessage(TableRow tableRow, boolean respectRequired)
        throws Exception {
      Message msg =
          TableRowToStorageApiProto.messageFromTableRow(
              schemaInformation, descriptor, tableRow, ignoreUnknownValues);
      return StorageApiWritePayload.of(msg.toByteArray());
    }
  };
}
