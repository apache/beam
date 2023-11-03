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

import static org.junit.Assert.assertEquals;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.bigquery.storage.v1.WriteStream.Type;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.rpc.Code;
import io.grpc.Status;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StreamAppendClient;
import org.apache.beam.sdk.io.gcp.bigquery.ErrorContainer;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy.Context;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiCDC;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowToStorageApiProto;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.FailsafeValueInSingleWindow;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;

/** A fake dataset service that can be serialized, for use in testReadFromTable. */
@Internal
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FakeDatasetService implements DatasetService, Serializable {
  // Table information must be static, as each ParDo will get a separate instance of
  // FakeDatasetServices, and they must all modify the same storage.
  static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table<
          String, String, Map<String, TableContainer>>
      tables;
  static Map<String, Stream> writeStreams;

  @Override
  public void close() throws Exception {}

  static class Stream {
    static class Entry {
      enum UpdateType {
        INSERT,
        UPSERT,
        DELETE
      };

      final TableRow tableRow;
      final UpdateType updateType;
      final long sqn;

      public Entry(TableRow tableRow, UpdateType updateType, long sqn) {
        this.tableRow = tableRow;
        this.updateType = updateType;
        this.sqn = sqn;
      }
    }

    final String streamName;
    final List<Entry> stream;
    final TableContainer tableContainer;
    final Type type;
    long nextFlushPosition;
    boolean finalized;
    TableSchema currentSchema;
    @Nullable TableSchema updatedSchema = null;

    Stream(String streamName, TableContainer tableContainer, Type type) {
      this.streamName = streamName;
      this.stream = Lists.newArrayList();
      this.tableContainer = tableContainer;
      this.currentSchema = tableContainer.getTable().getSchema();
      this.type = type;
      this.finalized = false;
      this.nextFlushPosition = 0;
    }

    void setUpdatedSchema(TableSchema tableSchema) {
      this.updatedSchema = tableSchema;
    }

    TableSchema getUpdatedSchema() {
      return this.updatedSchema;
    }

    WriteStream toWriteStream() {
      return WriteStream.newBuilder()
          .setName(streamName)
          .setType(type)
          .setTableSchema(TableRowToStorageApiProto.schemaToProtoTableSchema(currentSchema))
          .build();
    }

    long finalizeStream() {
      this.finalized = true;
      return stream.size();
    }

    void appendRows(long position, List<Entry> rowsToAppend) {
      if (finalized) {
        throw new RuntimeException("Stream already finalized.");
      }
      if (position != -1 && position != stream.size()) {
        throw new RuntimeException(
            "Bad append: "
                + position
                + " + for stream "
                + streamName
                + " expected "
                + stream.size());
      }
      stream.addAll(rowsToAppend);
      if (type == Type.COMMITTED) {
        rowsToAppend.forEach(this::applyEntry);
      }
    }

    void flush(long position) {
      Preconditions.checkState(type == Type.BUFFERED);
      Preconditions.checkState(!finalized);
      if (position >= stream.size()) {
        throw new RuntimeException("");
      }
      for (; nextFlushPosition <= position; ++nextFlushPosition) {
        applyEntry(stream.get((int) nextFlushPosition));
      }
    }

    void applyEntry(Entry entry) {
      switch (entry.updateType) {
        case INSERT:
          tableContainer.addRow(entry.tableRow, "");
          break;
        case UPSERT:
          tableContainer.upsertRow(entry.tableRow, entry.sqn);
          break;
        case DELETE:
          tableContainer.deleteRow(entry.tableRow, entry.sqn);
      }
    }

    void commit() {
      if (!finalized) {
        throw new RuntimeException("Can't commit unfinalized stream.");
      }
      Preconditions.checkState(type == Type.PENDING);
      stream.forEach(this::applyEntry);
    }
  }

  Function<TableRow, Boolean> shouldFailRow =
      (Function<TableRow, Boolean> & Serializable) tr -> false;
  Map<String, List<String>> insertErrors = Maps.newHashMap();

  // The counter for the number of insertions performed.
  static AtomicInteger insertCount;

  public static void setUp() {
    synchronized (FakeDatasetService.class) {
      tables = HashBasedTable.create();
      insertCount = new AtomicInteger(0);
      writeStreams = Maps.newHashMap();
      FakeJobService.setUp();
    }
  }

  public void setShouldFailRow(Function<TableRow, Boolean> shouldFailRow) {
    this.shouldFailRow = shouldFailRow;
  }

  @Override
  public Table getTable(TableReference tableRef) throws InterruptedException, IOException {
    if (tableRef.getProjectId() == null) {
      throw new NullPointerException(String.format("tableRef is missing projectId: %s", tableRef));
    }
    return getTable(tableRef, null);
  }

  @Override
  public Table getTable(TableReference tableRef, @Nullable List<String> selectedFields)
      throws InterruptedException, IOException {
    return getTable(tableRef, selectedFields, null);
  }

  @Override
  public Table getTable(
      TableReference tableRef,
      @Nullable List<String> selectedFields,
      @Nullable TableMetadataView view)
      throws InterruptedException, IOException {
    return getTableImpl(tableRef, selectedFields, view);
  }

  public Table getTableImpl(
      TableReference tableRef,
      @Nullable List<String> selectedFields,
      @Nullable TableMetadataView view)
      throws InterruptedException, IOException {
    synchronized (FakeDatasetService.class) {
      Map<String, TableContainer> dataset =
          tables.get(tableRef.getProjectId(), tableRef.getDatasetId());
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such dataset was set",
            tableRef.getProjectId(), tableRef.getDatasetId());
      }
      TableContainer tableContainer = dataset.get(tableRef.getTableId());

      return tableContainer == null ? null : tableContainer.getTable();
    }
  }

  public List<TableRow> getAllRows(String projectId, String datasetId, String tableId)
      throws InterruptedException, IOException {
    synchronized (FakeDatasetService.class) {
      return getTableContainer(projectId, datasetId, tableId).getRows();
    }
  }

  public List<String> getAllIds(String projectId, String datasetId, String tableId)
      throws InterruptedException, IOException {
    synchronized (FakeDatasetService.class) {
      return getTableContainer(projectId, datasetId, tableId).getIds();
    }
  }

  private TableContainer getTableContainer(String projectId, String datasetId, String tableId)
      throws InterruptedException, IOException {
    synchronized (FakeDatasetService.class) {
      Map<String, TableContainer> dataset = tables.get(projectId, datasetId);
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such dataset was set", projectId, datasetId);
      }
      TableContainer tableContainer = dataset.get(tableId);
      if (tableContainer == null) {
        throwNotFound(
            "Tried to get a table %s:%s.%s, but no such table was set",
            projectId, datasetId, tableId);
      }
      return tableContainer;
    }
  }

  @Override
  public void deleteTable(TableReference tableRef) throws IOException, InterruptedException {
    validateWholeTableReference(tableRef);
    synchronized (FakeDatasetService.class) {
      Map<String, TableContainer> dataset =
          tables.get(tableRef.getProjectId(), tableRef.getDatasetId());
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such table was set",
            tableRef.getProjectId(), tableRef.getDatasetId());
      }
      dataset.remove(tableRef.getTableId());
    }
  }

  /**
   * Validates a table reference for whole-table operations, such as create/delete/patch. Such
   * operations do not support partition decorators.
   */
  private static void validateWholeTableReference(TableReference tableReference)
      throws IOException {
    final Pattern tableRegexp = Pattern.compile("[-\\w]{1,1024}");
    if (!tableRegexp.matcher(tableReference.getTableId()).matches()) {
      throw new IOException(
          String.format(
              "invalid table ID %s. Table IDs must be alphanumeric "
                  + "(plus underscores) and must be at most 1024 characters long. Also, table"
                  + " decorators cannot be used.",
              tableReference.getTableId()));
    }
  }

  @Override
  public void createTable(Table table) throws IOException {
    TableReference tableReference = table.getTableReference();
    validateWholeTableReference(tableReference);
    synchronized (FakeDatasetService.class) {
      Map<String, TableContainer> dataset =
          tables.get(tableReference.getProjectId(), tableReference.getDatasetId());
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such table was set",
            tableReference.getProjectId(), tableReference.getDatasetId());
      }
      dataset.computeIfAbsent(
          tableReference.getTableId(),
          k -> {
            TableContainer tableContainer = new TableContainer(table);
            // Create the default stream.
            String streamName =
                String.format(
                    "projects/%s/datasets/%s/tables/%s/streams/_default",
                    tableReference.getProjectId(),
                    tableReference.getDatasetId(),
                    BigQueryHelpers.stripPartitionDecorator(tableReference.getTableId()));
            writeStreams.put(streamName, new Stream(streamName, tableContainer, Type.COMMITTED));

            return tableContainer;
          });
    }
  }

  public void updateTableSchema(TableReference tableReference, TableSchema tableSchema)
      throws IOException {
    validateWholeTableReference(tableReference);
    synchronized (FakeDatasetService.class) {
      Map<String, TableContainer> dataset =
          tables.get(tableReference.getProjectId(), tableReference.getDatasetId());
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such table was set",
            tableReference.getProjectId(), tableReference.getDatasetId());
      }
      @Nullable TableContainer tableContainer = dataset.get(tableReference.getTableId());
      if (tableContainer == null) {
        throwNotFound("Tried to get a table %s, but no such table existed", tableReference);
      }
      // TODO: Only allow "legal" schema changes.
      tableContainer.table.setSchema(tableSchema);

      for (Stream stream : writeStreams.values()) {
        if (stream.tableContainer == tableContainer) {
          stream.setUpdatedSchema(tableSchema);
        }
      }
    }
  }

  public void setPrimaryKey(TableReference tableReference, List<String> columns)
      throws IOException {
    validateWholeTableReference(tableReference);
    synchronized (FakeDatasetService.class) {
      Map<String, TableContainer> dataset =
          tables.get(tableReference.getProjectId(), tableReference.getDatasetId());
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such table was set",
            tableReference.getProjectId(), tableReference.getDatasetId());
      }
      @Nullable TableContainer tableContainer = dataset.get(tableReference.getTableId());
      if (tableContainer == null) {
        throwNotFound("Tried to get a table %s, but no such table existed", tableReference);
      }
      // Set primary key columns.
      tableContainer.setPrimaryKeyColumns(columns);
    }
  }

  @Override
  public boolean isTableEmpty(TableReference tableRef) throws IOException, InterruptedException {
    Long numBytes = getTable(tableRef).getNumBytes();
    return numBytes == null || numBytes == 0L;
  }

  @Override
  public Dataset getDataset(String projectId, String datasetId)
      throws IOException, InterruptedException {
    synchronized (FakeDatasetService.class) {
      Map<String, TableContainer> dataset = tables.get(projectId, datasetId);
      if (dataset == null) {
        throwNotFound(
            "Tried to get a dataset %s:%s, but no such table was set", projectId, datasetId);
      }
      return new Dataset()
          .setDatasetReference(
              new DatasetReference().setDatasetId(datasetId).setProjectId(projectId));
    }
  }

  @Override
  public void createDataset(
      String projectId,
      String datasetId,
      String location,
      String description,
      Long defaultTableExpirationMs /* ignored */)
      throws IOException, InterruptedException {
    synchronized (FakeDatasetService.class) {
      Map<String, TableContainer> dataset = tables.get(projectId, datasetId);
      if (dataset == null) {
        dataset = new HashMap<>();
        tables.put(projectId, datasetId, dataset);
      }
    }
  }

  @Override
  public void deleteDataset(String projectId, String datasetId)
      throws IOException, InterruptedException {
    synchronized (FakeDatasetService.class) {
      tables.remove(projectId, datasetId);
    }
  }

  public int getInsertCount() {
    return insertCount.get();
  }

  public long insertAll(
      TableReference ref, List<TableRow> rowList, @Nullable List<String> insertIdList)
      throws IOException, InterruptedException {
    List<FailsafeValueInSingleWindow<TableRow, TableRow>> windowedRows = Lists.newArrayList();
    for (TableRow row : rowList) {
      windowedRows.add(
          FailsafeValueInSingleWindow.of(
              row,
              GlobalWindow.TIMESTAMP_MAX_VALUE,
              GlobalWindow.INSTANCE,
              PaneInfo.ON_TIME_AND_ONLY_FIRING,
              row));
    }
    return insertAll(
        ref,
        windowedRows,
        insertIdList,
        InsertRetryPolicy.alwaysRetry(),
        null,
        null,
        false,
        false,
        false,
        null);
  }

  @Override
  public <T> long insertAll(
      TableReference ref,
      List<FailsafeValueInSingleWindow<TableRow, TableRow>> rowList,
      @Nullable List<String> insertIdList,
      InsertRetryPolicy retryPolicy,
      List<ValueInSingleWindow<T>> failedInserts,
      ErrorContainer<T> errorContainer,
      boolean skipInvalidRows,
      boolean ignoreUnknownValues,
      boolean ignoreInsertIds,
      List<ValueInSingleWindow<TableRow>> successfulRows)
      throws IOException, InterruptedException {
    Map<TableRow, List<TableDataInsertAllResponse.InsertErrors>> insertErrors = getInsertErrors();
    synchronized (FakeDatasetService.class) {
      if (ignoreInsertIds) {
        insertIdList = null;
      }

      if (insertIdList != null) {
        assertEquals(rowList.size(), insertIdList.size());
      }

      long dataSize = 0;
      TableContainer tableContainer =
          getTableContainer(
              ref.getProjectId(),
              ref.getDatasetId(),
              BigQueryHelpers.stripPartitionDecorator(ref.getTableId()));
      for (int i = 0; i < rowList.size(); ++i) {
        TableRow row = rowList.get(i).getValue();
        List<TableDataInsertAllResponse.InsertErrors> allErrors = insertErrors.get(row);
        boolean shouldInsert = true;
        if (allErrors != null) {
          for (TableDataInsertAllResponse.InsertErrors errors : allErrors) {
            if (!retryPolicy.shouldRetry(new Context(errors))) {
              shouldInsert = false;
            }
          }
        }
        if (shouldInsert) {
          if (insertIdList == null) {
            dataSize += tableContainer.addRow(row, null);
          } else {
            dataSize += tableContainer.addRow(row, insertIdList.get(i));
          }
          if (successfulRows != null) {
            successfulRows.add(
                ValueInSingleWindow.of(
                    row,
                    rowList.get(i).getTimestamp(),
                    rowList.get(i).getWindow(),
                    rowList.get(i).getPane()));
          }
        } else {
          errorContainer.add(
              failedInserts, allErrors.get(allErrors.size() - 1), ref, rowList.get(i));
        }
      }
      insertCount.addAndGet(1);
      return dataSize;
    }
  }

  @Override
  public Table patchTableDescription(
      TableReference tableReference, @Nullable String tableDescription)
      throws IOException, InterruptedException {
    validateWholeTableReference(tableReference);
    synchronized (FakeDatasetService.class) {
      TableContainer tableContainer =
          getTableContainer(
              tableReference.getProjectId(),
              tableReference.getDatasetId(),
              tableReference.getTableId());
      tableContainer.getTable().setDescription(tableDescription);
      return tableContainer.getTable();
    }
  }

  @Override
  public WriteStream createWriteStream(String tableUrn, Type type) throws InterruptedException {
    try {
      TableReference tableReference =
          BigQueryHelpers.parseTableUrn(BigQueryHelpers.stripPartitionDecorator(tableUrn));
      synchronized (FakeDatasetService.class) {
        TableContainer tableContainer =
            getTableContainer(
                tableReference.getProjectId(),
                tableReference.getDatasetId(),
                tableReference.getTableId());
        String streamName = UUID.randomUUID().toString();
        Stream stream = new Stream(streamName, tableContainer, type);
        writeStreams.put(streamName, stream);
        return stream.toWriteStream();
      }
    } catch (IOException e) {
      // TODO(relax): Return the exact error that BigQuery returns.
      throw new ApiException(e, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
    }
  }

  @Override
  @Nullable
  public WriteStream getWriteStream(String streamName) {
    synchronized (FakeDatasetService.class) {
      @Nullable Stream stream = writeStreams.get(streamName);
      if (stream != null) {
        return stream.toWriteStream();
      }
    }
    // TODO(relax): Return the exact error that BigQuery returns.
    throw new ApiException(null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
  }

  @Override
  public StreamAppendClient getStreamAppendClient(
      String streamName,
      DescriptorProtos.DescriptorProto descriptor,
      boolean useConnectionPool,
      AppendRowsRequest.MissingValueInterpretation missingValueInterpretation)
      throws Exception {
    return new StreamAppendClient() {
      private Descriptor protoDescriptor;
      private TableSchema currentSchema;
      private @Nullable com.google.cloud.bigquery.storage.v1.TableSchema updatedSchema;

      private boolean usedForInsert = false;
      private boolean usedForUpdate = false;

      {
        this.protoDescriptor = TableRowToStorageApiProto.wrapDescriptorProto(descriptor);

        synchronized (FakeDatasetService.class) {
          Stream stream = writeStreams.get(streamName);
          if (stream == null) {
            // TODO(relax): Return the exact error that BigQuery returns.
            throw new ApiException(null, GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
          }
          currentSchema = stream.tableContainer.getTable().getSchema();
        }
      }

      @Override
      public ApiFuture<AppendRowsResponse> appendRows(long offset, ProtoRows rows)
          throws Exception {
        AppendRowsResponse.Builder responseBuilder = AppendRowsResponse.newBuilder();
        synchronized (FakeDatasetService.class) {
          Stream stream = writeStreams.get(streamName);
          if (stream == null) {
            throw new RuntimeException("No such stream: " + streamName);
          }
          List<Stream.Entry> streamEntries =
              Lists.newArrayListWithExpectedSize(rows.getSerializedRowsCount());
          Map<Integer, String> rowIndexToErrorMessage = Maps.newHashMap();
          for (int i = 0; i < rows.getSerializedRowsCount(); ++i) {
            ByteString bytes = rows.getSerializedRows(i);
            DynamicMessage msg = DynamicMessage.parseFrom(protoDescriptor, bytes);
            if (msg.getUnknownFields() != null && !msg.getUnknownFields().asMap().isEmpty()) {
              throw new RuntimeException("Unknown fields set in append! " + msg.getUnknownFields());
            }
            TableRow tableRow =
                TableRowToStorageApiProto.tableRowFromMessage(
                    DynamicMessage.parseFrom(protoDescriptor, bytes), false);
            if (shouldFailRow.apply(tableRow)) {
              rowIndexToErrorMessage.put(i, "Failing row " + tableRow.toPrettyString());
            }
            String insertTypeStr = null;
            long changeSequenceNum = -1;
            Descriptors.FieldDescriptor fieldDescriptor =
                protoDescriptor.findFieldByName(StorageApiCDC.CHANGE_TYPE_COLUMN);
            if (fieldDescriptor != null) {
              insertTypeStr = (String) msg.getField(fieldDescriptor);
            }
            fieldDescriptor = protoDescriptor.findFieldByName(StorageApiCDC.CHANGE_SQN_COLUMN);
            if (fieldDescriptor != null) {
              changeSequenceNum = (long) msg.getField(fieldDescriptor);
            }
            Stream.Entry.UpdateType insertType = Stream.Entry.UpdateType.INSERT;
            if (insertTypeStr != null) {
              insertType = Stream.Entry.UpdateType.valueOf(insertTypeStr);
            }
            if (insertType == Stream.Entry.UpdateType.INSERT) {
              Preconditions.checkArgument(
                  !usedForUpdate, "Stream can't be used for update and insert.");
              usedForInsert = true;
            } else {
              Preconditions.checkArgument(
                  !usedForInsert, "Stream can't be used for update and insert.");
              usedForUpdate = true;
            }
            streamEntries.add(new Stream.Entry(tableRow, insertType, changeSequenceNum));
          }
          if (!rowIndexToErrorMessage.isEmpty()) {
            return ApiFutures.immediateFailedFuture(
                new Exceptions.AppendSerializtionError(
                    Code.INVALID_ARGUMENT.getNumber(),
                    "Append serialization failed for writer: " + streamName,
                    stream.streamName,
                    rowIndexToErrorMessage));
          }
          stream.appendRows(offset, streamEntries);
          if (stream.getUpdatedSchema() != null) {
            com.google.cloud.bigquery.storage.v1.TableSchema newSchema =
                TableRowToStorageApiProto.schemaToProtoTableSchema(stream.getUpdatedSchema());
            responseBuilder.setUpdatedSchema(newSchema);
            if (this.updatedSchema == null) {
              this.updatedSchema = newSchema;
            }
          }
        }
        return ApiFutures.immediateFuture(responseBuilder.build());
      }

      @Override
      public com.google.cloud.bigquery.storage.v1.@org.checkerframework.checker.nullness.qual
              .Nullable
          TableSchema getUpdatedSchema() {
        return this.updatedSchema;
      }

      @Override
      public void close() throws Exception {}

      @Override
      public void pin() {}

      @Override
      public void unpin() throws Exception {}
    };
  }

  @Override
  public ApiFuture<FlushRowsResponse> flush(String streamName, long offset) {
    synchronized (FakeDatasetService.class) {
      Stream stream = writeStreams.get(streamName);
      if (stream == null) {
        throw new RuntimeException("No such stream: " + streamName);
      }
      stream.flush(offset);
    }
    return ApiFutures.immediateFuture(FlushRowsResponse.newBuilder().build());
  }

  @Override
  public ApiFuture<FinalizeWriteStreamResponse> finalizeWriteStream(String streamName) {
    synchronized (FakeDatasetService.class) {
      Stream stream = writeStreams.get(streamName);
      if (stream == null) {
        throw new RuntimeException("No such stream: " + streamName);
      }
      long numRows = stream.finalizeStream();
      return ApiFutures.immediateFuture(
          FinalizeWriteStreamResponse.newBuilder().setRowCount(numRows).build());
    }
  }

  @Override
  public ApiFuture<BatchCommitWriteStreamsResponse> commitWriteStreams(
      String tableUrn, Iterable<String> writeStreamNames) {
    synchronized (FakeDatasetService.class) {
      for (String streamName : writeStreamNames) {
        Stream stream = writeStreams.get(streamName);
        if (stream == null) {
          throw new RuntimeException("No such stream: " + streamName);
        }
        stream.commit();
      }
    }
    return ApiFutures.immediateFuture(
        BatchCommitWriteStreamsResponse.newBuilder()
            .setCommitTime(Timestamp.newBuilder().build())
            .build());
  }

  /**
   * Cause a given {@link TableRow} object to fail when it's inserted. The errors link the list will
   * be returned on subsequent retries, and the insert will succeed when the errors run out.
   */
  public void failOnInsert(
      Map<TableRow, List<TableDataInsertAllResponse.InsertErrors>> insertErrors) {
    synchronized (FakeDatasetService.class) {
      for (Map.Entry<TableRow, List<TableDataInsertAllResponse.InsertErrors>> entry :
          insertErrors.entrySet()) {
        List<String> errorStrings = Lists.newArrayList();
        for (TableDataInsertAllResponse.InsertErrors errors : entry.getValue()) {
          errorStrings.add(BigQueryHelpers.toJsonString(errors));
        }
        this.insertErrors.put(BigQueryHelpers.toJsonString(entry.getKey()), errorStrings);
      }
    }
  }

  Map<TableRow, List<TableDataInsertAllResponse.InsertErrors>> getInsertErrors() {
    Map<TableRow, List<TableDataInsertAllResponse.InsertErrors>> parsedInsertErrors =
        Maps.newHashMap();
    synchronized (FakeDatasetService.class) {
      for (Map.Entry<String, List<String>> entry : this.insertErrors.entrySet()) {
        TableRow tableRow = BigQueryHelpers.fromJsonString(entry.getKey(), TableRow.class);
        List<TableDataInsertAllResponse.InsertErrors> allErrors = Lists.newArrayList();
        for (String errorsString : entry.getValue()) {
          allErrors.add(
              BigQueryHelpers.fromJsonString(
                  errorsString, TableDataInsertAllResponse.InsertErrors.class));
        }
        parsedInsertErrors.put(tableRow, allErrors);
      }
    }
    return parsedInsertErrors;
  }

  @FormatMethod
  void throwNotFound(@FormatString String format, Object... args) throws IOException {
    throw new IOException(
        String.format(format, args),
        new GoogleJsonResponseException.Builder(404, String.format(format, args), new HttpHeaders())
            .build());
  }
}
