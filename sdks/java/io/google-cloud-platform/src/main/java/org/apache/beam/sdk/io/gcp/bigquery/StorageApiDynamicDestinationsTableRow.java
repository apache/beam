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
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

public class StorageApiDynamicDestinationsTableRow<T, DestinationT extends @NonNull Object>
    extends StorageApiDynamicDestinations<T, DestinationT> {
  private final BigQueryIO.TableRowFormatFunction<T> formatFunction;
  private final BigQueryIO.@Nullable TableRowFormatFunction<T> formatRecordOnFailureFunction;

  private final boolean usesCdc;
  private final CreateDisposition createDisposition;
  private final boolean ignoreUnknownValues;
  private final boolean autoSchemaUpdates;
  private final Set<BigQueryIO.Write.SchemaUpdateOption> schemaUpdateOptions;
  private static final TableSchemaCache SCHEMA_CACHE =
      new TableSchemaCache(Duration.standardSeconds(1));

  static {
    SCHEMA_CACHE.start();
  }

  StorageApiDynamicDestinationsTableRow(
      DynamicDestinations<T, DestinationT> inner,
      BigQueryIO.TableRowFormatFunction<T> formatFunction,
      BigQueryIO.@Nullable TableRowFormatFunction<T> formatRecordOnFailureFunction,
      boolean usesCdc,
      CreateDisposition createDisposition,
      boolean ignoreUnknownValues,
      boolean autoSchemaUpdates,
      Set<BigQueryIO.Write.SchemaUpdateOption> schemaUpdateOptions) {
    super(inner);
    this.formatFunction = formatFunction;
    this.formatRecordOnFailureFunction = formatRecordOnFailureFunction;
    this.usesCdc = usesCdc;
    this.createDisposition = createDisposition;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.autoSchemaUpdates = autoSchemaUpdates;
    this.schemaUpdateOptions = schemaUpdateOptions;
  }

  static void clearSchemaCache() throws ExecutionException, InterruptedException {
    SCHEMA_CACHE.clear();
  }

  @Override
  public MessageConverter<T> getMessageConverter(
      DestinationT destination,
      PipelineOptions options,
      DatasetService datasetService,
      BigQueryServices.WriteStreamService writeStreamService) {
    SerializableFunction<@Nullable TableSchema, TableRowConverter> getConverter =
        tableSchema -> {
          try {
            return new TableRowConverter(destination, datasetService, tableSchema);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    return schemaUpdateOptions.isEmpty()
        ? getConverter.apply(getSchema(destination))
        : new SchemaUpgradingTableRowConverter(
            getConverter, options, datasetService, writeStreamService);
  }

  // This is a wrapper class used when schemaUpdateOptions are set.
  class SchemaUpgradingTableRowConverter implements MessageConverter<T> {
    private final SerializableFunction<@Nullable TableSchema, TableRowConverter> getConverter;
    private final DatasetService datasetService;
    private final BigQueryServices.WriteStreamService writeStreamService;
    private final BigQueryOptions bigQueryOptions;
    private AtomicReference<TableRowConverter> delegate = new AtomicReference<>();

    SchemaUpgradingTableRowConverter(
        SerializableFunction<@Nullable TableSchema, TableRowConverter> getConverter,
        PipelineOptions options,
        DatasetService datasetService,
        BigQueryServices.WriteStreamService writeStreamService) {
      this.getConverter = getConverter;
      this.datasetService = datasetService;
      this.writeStreamService = writeStreamService;
      this.bigQueryOptions = options.as(BigQueryOptions.class);
      // Pass in null - force us to look up the actual table schema.
      this.delegate.set(getConverter.apply(null));
    }

    @Override
    public com.google.cloud.bigquery.storage.v1.TableSchema getTableSchema() {
      return delegate.get().getTableSchema();
    }

    @Override
    public DescriptorProtos.DescriptorProto getDescriptor(boolean includeCdcColumns)
        throws Exception {
      return delegate.get().getDescriptor(includeCdcColumns);
    }

    @Override
    public StorageApiWritePayload toMessage(
        T element,
        @Nullable RowMutationInformation rowMutationInformation,
        TableRowToStorageApiProto.ErrorCollector collectedExceptions)
        throws Exception {
      TableRowConverter converter = delegate.get();
      StorageApiWritePayload payload =
          converter.toMessage(element, rowMutationInformation, collectedExceptions);
      // Set the schema hash on the payload so the next transform knows whether it has an
      // out-of-date schema.
      payload = payload.toBuilder().setSchemaHash(converter.getSchemaHash()).build();

      return payload;
    }

    @Override
    public void updateSchemaFromTable() throws IOException, InterruptedException {
      SCHEMA_CACHE.refreshSchema(
          delegate.get().tableReference, datasetService, writeStreamService, bigQueryOptions);
      // Recycle the internal MessageConverter so that we pick up the new schema from the cache.
      this.delegate.set(getConverter.apply(null));
    }

    @Override
    public TableRow toFailsafeTableRow(T element) {
      return delegate.get().toFailsafeTableRow(element);
    }
  }

  class TableRowConverter implements MessageConverter<T> {
    final TableReference tableReference;
    final @Nullable TableSchema tableSchema;
    final com.google.cloud.bigquery.storage.v1.TableSchema protoTableSchema;
    final Supplier<byte[]> getSchemaHash;
    final TableRowToStorageApiProto.SchemaInformation schemaInformation;
    final Descriptor descriptor;
    final @Nullable Descriptor cdcDescriptor;

    TableRowConverter(
        DestinationT destination,
        DatasetService datasetService,
        @Nullable TableSchema localTableSchema)
        throws Exception {
      this.tableReference = getTable(destination).getTableReference();
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
      this.getSchemaHash =
          Suppliers.memoize(() -> TableRowToStorageApiProto.tableSchemaHash(this.protoTableSchema));
      schemaInformation =
          TableRowToStorageApiProto.SchemaInformation.fromTableSchema(protoTableSchema);
      // If autoSchemaUpdates == true, then generate a descriptor where all the fields are optional.
      // This allows us to support field relaxation downstream.
      descriptor =
          TableRowToStorageApiProto.getDescriptorFromTableSchema(
              Preconditions.checkStateNotNull(tableSchema), !autoSchemaUpdates, false);
      if (usesCdc) {
        cdcDescriptor =
            TableRowToStorageApiProto.getDescriptorFromTableSchema(
                Preconditions.checkStateNotNull(tableSchema), !autoSchemaUpdates, true);
      } else {
        cdcDescriptor = null;
      }
    }

    @Override
    public void updateSchemaFromTable() throws IOException, InterruptedException {}

    @Override
    public com.google.cloud.bigquery.storage.v1.TableSchema getTableSchema() {
      return protoTableSchema;
    }

    byte[] getSchemaHash() {
      return getSchemaHash.get();
    }

    @Override
    public DescriptorProtos.DescriptorProto getDescriptor(boolean includeCdcColumns)
        throws Exception {
      return cdcDescriptor != null ? cdcDescriptor.toProto() : descriptor.toProto();
    }

    @Override
    public TableRow toFailsafeTableRow(T element) {
      if (formatRecordOnFailureFunction != null) {
        return formatRecordOnFailureFunction.apply(schemaInformation, element);
      } else {
        return formatFunction.apply(schemaInformation, element);
      }
    }

    @Override
    public StorageApiWritePayload toMessage(
        T element,
        @Nullable RowMutationInformation rowMutationInformation,
        TableRowToStorageApiProto.ErrorCollector collectedExceptions)
        throws Exception {
      TableRow tableRow = formatFunction.apply(schemaInformation, element);

      String changeType = null;
      String changeSequenceNum = null;
      Descriptor descriptorToUse = descriptor;
      if (rowMutationInformation != null) {
        changeType = rowMutationInformation.getMutationType().toString();
        changeSequenceNum = rowMutationInformation.getChangeSequenceNumber();
        descriptorToUse = Preconditions.checkStateNotNull(cdcDescriptor);
      }
      // If autoSchemaUpdates==true, then we allow unknown values at this step and insert them into
      // the unknownFields variable. This allows us to handle schema updates in the write stage.
      boolean ignoreUnknown = ignoreUnknownValues || autoSchemaUpdates;
      @Nullable TableRow unknownFields = autoSchemaUpdates ? new TableRow() : null;
      boolean allowMissingFields = autoSchemaUpdates;
      @Nullable
      Message msg =
          TableRowToStorageApiProto.messageFromTableRow(
              schemaInformation,
              descriptorToUse,
              tableRow,
              ignoreUnknown,
              allowMissingFields,
              unknownFields,
              changeType,
              changeSequenceNum,
              collectedExceptions);
      return StorageApiWritePayload.of(
          msg == null ? new byte[0] : msg.toByteArray(),
          unknownFields,
          formatRecordOnFailureFunction != null ? toFailsafeTableRow(element) : null);
    }
  };
}
