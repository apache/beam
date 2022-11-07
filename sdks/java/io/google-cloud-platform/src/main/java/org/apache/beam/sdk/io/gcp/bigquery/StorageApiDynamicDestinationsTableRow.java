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
import org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto.SchemaTooNarrowException;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageApiDynamicDestinationsTableRow<T, DestinationT extends @NonNull Object>
    extends StorageApiDynamicDestinations<T, DestinationT> {
  private final SerializableFunction<T, TableRow> formatFunction;
  private final CreateDisposition createDisposition;
  private final boolean ignoreUnknownValues;
  private final int schemaUpdateRetries;
  private final boolean autoSchemaUpdates;
  private static final TableSchemaCache SCHEMA_CACHE =
      new TableSchemaCache(Duration.standardSeconds(1));
  private static final Logger LOG =
      LoggerFactory.getLogger(StorageApiDynamicDestinationsTableRow.class);

  static {
    SCHEMA_CACHE.start();
  }

  StorageApiDynamicDestinationsTableRow(
      DynamicDestinations<T, DestinationT> inner,
      SerializableFunction<T, TableRow> formatFunction,
      CreateDisposition createDisposition,
      boolean ignoreUnknownValues,
      int schemaUpdateRetries,
      boolean autoSchemaUpdates) {
    super(inner);
    this.formatFunction = formatFunction;
    this.createDisposition = createDisposition;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.schemaUpdateRetries = schemaUpdateRetries;
    this.autoSchemaUpdates = autoSchemaUpdates;
  }

  static void clearSchemaCache() throws ExecutionException, InterruptedException {
    SCHEMA_CACHE.clear();
  }

  @Override
  public MessageConverter<T> getMessageConverter(
      DestinationT destination, DatasetService datasetService) throws Exception {
    return new MessageConverter<T>() {
      @Nullable TableSchema tableSchema;
      TableRowToStorageApiProto.SchemaInformation schemaInformation;
      Descriptor descriptor;
      long descriptorHash;

      {
        tableSchema = getSchema(destination);
        TableReference tableReference = getTable(destination).getTableReference();
        if (tableSchema == null) {
          // If the table already exists, then try and fetch the schema from the existing
          // table.
          tableSchema = SCHEMA_CACHE.getSchema(tableReference, datasetService);
          if (tableSchema == null) {
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
          tableSchema =
              MoreObjects.firstNonNull(
                  SCHEMA_CACHE.putSchemaIfAbsent(tableReference, tableSchema), tableSchema);
        }
        schemaInformation =
            TableRowToStorageApiProto.SchemaInformation.fromTableSchema(tableSchema);
        descriptor = TableRowToStorageApiProto.getDescriptorFromTableSchema(tableSchema);
        descriptorHash = BigQueryUtils.hashSchemaDescriptorDeterministic(descriptor);
      }

      @Override
      public DescriptorWrapper getSchemaDescriptor() {
        synchronized (this) {
          return new DescriptorWrapper(descriptor, descriptorHash);
        }
      }

      @Override
      public void refreshSchema(long expectedHash) throws Exception {
        // When a table is updated, all streams writing to that table will try to refresh the
        // schema. Since we don't want them all querying the table for the schema, keep track of
        // the expected hash and return if it already matches.
        synchronized (this) {
          if (expectedHash == descriptorHash) {
            return;
          }
        }
        refreshSchemaInternal();
      }

      public void refreshSchemaInternal() throws Exception {
        TableReference tableReference = getTable(destination).getTableReference();
        SCHEMA_CACHE.refreshSchema(tableReference, datasetService);
        TableSchema newSchema = SCHEMA_CACHE.getSchema(tableReference, datasetService);
        if (newSchema == null) {
          throw new RuntimeException("BigQuery table " + tableReference + " not found");
        }
        synchronized (this) {
          tableSchema = newSchema;
          schemaInformation =
              TableRowToStorageApiProto.SchemaInformation.fromTableSchema(tableSchema);
          descriptor = TableRowToStorageApiProto.getDescriptorFromTableSchema(tableSchema);
          long newHash = BigQueryUtils.hashSchemaDescriptorDeterministic(descriptor);
          if (descriptorHash != newHash) {
            LOG.info(
                "Refreshed table "
                    + BigQueryHelpers.toTableSpec(tableReference)
                    + " has a new schema.");
          }
          descriptorHash = newHash;
        }
      }

      @Override
      public TableRow toTableRow(T element) {
        return formatFunction.apply(element);
      }

      @Override
      public StorageApiWritePayload toMessage(T element) throws Exception {
        int attempt = 0;
        do {
          TableRowToStorageApiProto.SchemaInformation localSchemaInformation;
          Descriptor localDescriptor;
          long localDescriptorHash;
          synchronized (this) {
            localSchemaInformation = schemaInformation;
            localDescriptor = descriptor;
            localDescriptorHash = descriptorHash;
          }
          try {
            Message msg =
                TableRowToStorageApiProto.messageFromTableRow(
                    localSchemaInformation,
                    localDescriptor,
                    formatFunction.apply(element),
                    ignoreUnknownValues);
            return new AutoValue_StorageApiWritePayload(msg.toByteArray(), localDescriptorHash);
          } catch (SchemaTooNarrowException e) {
            if (!autoSchemaUpdates || attempt > schemaUpdateRetries) {
              throw e;
            }
            // The input record has fields not found in the schema, and ignoreUnknownValues=false.
            // It's possible that the user has updated the target table with a wider schema. Try
            // to read the target's table schema to see if that is the case.
            refreshSchemaInternal();
            ++attempt;
          }
        } while (true);
      }
    };
  }
}
