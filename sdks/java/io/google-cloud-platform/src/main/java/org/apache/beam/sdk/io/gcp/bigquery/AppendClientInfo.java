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

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;

/**
 * Container class used by {@link StorageApiWritesShardedRecords} and {@link
 * StorageApiWritesShardedRecords} to enapsulate a destination {@link TableSchema} along with a
 * {@link BigQueryServices.StreamAppendClient} and other objects needed to write records.
 */
@AutoValue
abstract class AppendClientInfo {
  abstract @Nullable BigQueryServices.StreamAppendClient getStreamAppendClient();

  abstract TableSchema getTableSchema();

  abstract Consumer<BigQueryServices.StreamAppendClient> getCloseAppendClient();

  abstract com.google.api.services.bigquery.model.TableSchema getJsonTableSchema();

  abstract TableRowToStorageApiProto.SchemaInformation getSchemaInformation();

  abstract @Nullable String getStreamName();

  abstract Descriptors.Descriptor getDescriptor();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setStreamAppendClient(@Nullable BigQueryServices.StreamAppendClient value);

    abstract Builder setTableSchema(TableSchema value);

    abstract Builder setCloseAppendClient(Consumer<BigQueryServices.StreamAppendClient> value);

    abstract Builder setJsonTableSchema(com.google.api.services.bigquery.model.TableSchema value);

    abstract Builder setSchemaInformation(TableRowToStorageApiProto.SchemaInformation value);

    abstract Builder setDescriptor(Descriptors.Descriptor value);

    abstract Builder setStreamName(@Nullable String name);

    abstract AppendClientInfo build();
  };

  abstract Builder toBuilder();

  static AppendClientInfo of(
      TableSchema tableSchema,
      Consumer<BigQueryServices.StreamAppendClient> closeAppendClient,
      boolean includeCdcColumns)
      throws Exception {
    return new AutoValue_AppendClientInfo.Builder()
        .setTableSchema(tableSchema)
        .setCloseAppendClient(closeAppendClient)
        .setJsonTableSchema(TableRowToStorageApiProto.protoSchemaToTableSchema(tableSchema))
        .setSchemaInformation(
            TableRowToStorageApiProto.SchemaInformation.fromTableSchema(tableSchema))
        .setDescriptor(
            TableRowToStorageApiProto.getDescriptorFromTableSchema(
                tableSchema, true, includeCdcColumns))
        .build();
  }

  public AppendClientInfo withNoAppendClient() {
    return toBuilder().setStreamAppendClient(null).build();
  }

  public AppendClientInfo withAppendClient(
      BigQueryServices.DatasetService datasetService,
      Supplier<String> getStreamName,
      boolean useConnectionPool)
      throws Exception {
    if (getStreamAppendClient() != null) {
      return this;
    } else {
      String streamName = getStreamName.get();
      return toBuilder()
          .setStreamName(streamName)
          .setStreamAppendClient(
              datasetService.getStreamAppendClient(streamName, getDescriptor(), useConnectionPool))
          .build();
    }
  }

  public void close() {
    BigQueryServices.StreamAppendClient client = getStreamAppendClient();
    if (client != null) {
      getCloseAppendClient().accept(client);
    }
  }

  boolean hasSchemaChanged(TableSchema updatedTableSchema) {
    return updatedTableSchema.hashCode() != getTableSchema().hashCode();
  }

  public ByteString encodeUnknownFields(TableRow unknown, boolean ignoreUnknownValues)
      throws TableRowToStorageApiProto.SchemaConversionException {
    Message msg =
        TableRowToStorageApiProto.messageFromTableRow(
            getSchemaInformation(),
            getDescriptorIgnoreRequired(),
            unknown,
            ignoreUnknownValues,
            true,
            null,
            null,
            -1);
    return msg.toByteString();
  }

  @Memoized
  Descriptors.Descriptor getDescriptorIgnoreRequired() {
    try {
      // Ignore CDC columns since this is just for unknown fields.
      return TableRowToStorageApiProto.getDescriptorFromTableSchema(getTableSchema(), false, false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public TableRow toTableRow(ByteString protoBytes) {
    try {
      return TableRowToStorageApiProto.tableRowFromMessage(
          DynamicMessage.parseFrom(getDescriptor(), protoBytes), true);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
