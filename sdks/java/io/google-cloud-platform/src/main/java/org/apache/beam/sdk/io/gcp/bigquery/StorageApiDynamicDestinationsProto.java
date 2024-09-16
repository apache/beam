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
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.lang.reflect.InvocationTargetException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.checkerframework.checker.nullness.qual.NonNull;

/** Storage API DynamicDestinations used when the input is a compiled protocol buffer. */
class StorageApiDynamicDestinationsProto<T extends Message, DestinationT extends @NonNull Object>
    extends StorageApiDynamicDestinations<T, DestinationT> {
  private final DescriptorProtos.DescriptorProto descriptorProto;
  private final @Nullable SerializableFunction<T, TableRow> formatRecordOnFailureFunction;

  @SuppressWarnings({"unchecked", "nullness"})
  StorageApiDynamicDestinationsProto(
      DynamicDestinations<T, DestinationT> inner,
      Class<T> protoClass,
      @Nullable SerializableFunction<T, TableRow> formatRecordOnFailureFunction) {
    super(inner);
    try {
      this.formatRecordOnFailureFunction = formatRecordOnFailureFunction;
      this.descriptorProto =
          fixNestedTypes(
              (Descriptors.Descriptor)
                  Preconditions.checkStateNotNull(protoClass.getMethod("getDescriptor"))
                      .invoke(null));
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public MessageConverter<T> getMessageConverter(
      DestinationT destination, DatasetService datasetService) throws Exception {
    return new Converter(
        TableRowToStorageApiProto.schemaToProtoTableSchema(
            Preconditions.checkStateNotNull(getSchema(destination))));
  }

  class Converter implements MessageConverter<T> {
    TableSchema tableSchema;

    Converter(TableSchema tableSchema) {
      this.tableSchema = tableSchema;
    }

    @Override
    public TableSchema getTableSchema() {
      return tableSchema;
    }

    @Override
    public DescriptorProtos.DescriptorProto getDescriptor(boolean includeCdcColumns)
        throws Exception {
      org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument(
          !includeCdcColumns);
      return descriptorProto;
    }

    @Override
    public StorageApiWritePayload toMessage(
        T element, @Nullable RowMutationInformation rowMutationInformation) throws Exception {
      // NB: What makes this path efficient is that the storage API directly understands protos, so
      // we can forward
      // the through directly. This means that we don't currently support ignoreUnknownValues or
      // autoUpdateSchema.
      return StorageApiWritePayload.of(
          element.toByteArray(),
          null,
          formatRecordOnFailureFunction != null ? toFailsafeTableRow(element) : null);
    }

    @Override
    public TableRow toFailsafeTableRow(T element) {
      if (formatRecordOnFailureFunction != null) {
        return formatRecordOnFailureFunction.apply(element);
      } else {
        try {
          return TableRowToStorageApiProto.tableRowFromMessage(
              DynamicMessage.parseFrom(
                  TableRowToStorageApiProto.wrapDescriptorProto(descriptorProto),
                  element.toByteArray()),
              true,
              Predicates.alwaysTrue());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  };

  private static DescriptorProtos.DescriptorProto fixNestedTypes(
      Descriptors.Descriptor descriptor) {
    return ProtoSchemaConverter.convert(descriptor).getProtoDescriptor();
  }
}
