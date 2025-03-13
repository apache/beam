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
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

/** Storage API DynamicDestinations used when the input is a Beam Row. */
class StorageApiDynamicDestinationsBeamRow<T, DestinationT extends @NonNull Object>
    extends StorageApiDynamicDestinations<T, DestinationT> {
  private final TableSchema tableSchema;
  private final SerializableFunction<T, Row> toRow;
  private final @Nullable SerializableFunction<T, TableRow> formatRecordOnFailureFunction;

  private final boolean usesCdc;

  StorageApiDynamicDestinationsBeamRow(
      DynamicDestinations<T, DestinationT> inner,
      Schema schema,
      SerializableFunction<T, Row> toRow,
      @Nullable SerializableFunction<T, TableRow> formatRecordOnFailureFunction,
      boolean usesCdc) {
    super(inner);
    this.tableSchema = BeamRowToStorageApiProto.protoTableSchemaFromBeamSchema(schema);
    this.toRow = toRow;
    this.formatRecordOnFailureFunction = formatRecordOnFailureFunction;
    this.usesCdc = usesCdc;
  }

  @Override
  public MessageConverter<T> getMessageConverter(
      DestinationT destination, DatasetService datasetService) throws Exception {
    return new BeamRowConverter();
  }

  class BeamRowConverter implements MessageConverter<T> {
    final Descriptor descriptor;
    final @Nullable Descriptor cdcDescriptor;

    BeamRowConverter() throws Exception {
      this.descriptor =
          TableRowToStorageApiProto.getDescriptorFromTableSchema(tableSchema, true, false);
      if (usesCdc) {
        cdcDescriptor =
            TableRowToStorageApiProto.getDescriptorFromTableSchema(
                Preconditions.checkStateNotNull(tableSchema), true, true);
      } else {
        cdcDescriptor = null;
      }
    }

    @Override
    public TableSchema getTableSchema() {
      return tableSchema;
    }

    @Override
    public DescriptorProtos.DescriptorProto getDescriptor(boolean includeCdcColumns) {
      return cdcDescriptor != null ? cdcDescriptor.toProto() : descriptor.toProto();
    }

    @Override
    @SuppressWarnings("nullness")
    public StorageApiWritePayload toMessage(
        T element, @Nullable RowMutationInformation rowMutationInformation) throws Exception {
      String changeType = null;
      String changeSequenceNum = null;
      Descriptor descriptorToUse = descriptor;
      if (rowMutationInformation != null) {
        changeType = rowMutationInformation.getMutationType().toString();
        changeSequenceNum = rowMutationInformation.getChangeSequenceNumber();
        descriptorToUse = Preconditions.checkStateNotNull(cdcDescriptor);
      }
      Message msg =
          BeamRowToStorageApiProto.messageFromBeamRow(
              descriptorToUse, toRow.apply(element), changeType, changeSequenceNum);
      return StorageApiWritePayload.of(
          msg.toByteArray(),
          null,
          formatRecordOnFailureFunction != null ? toFailsafeTableRow(element) : null);
    }

    @Override
    public TableRow toFailsafeTableRow(T element) {
      if (formatRecordOnFailureFunction != null) {
        return formatRecordOnFailureFunction.apply(element);
      } else {
        return BigQueryUtils.toTableRow(toRow.apply(element));
      }
    }
  };
}
