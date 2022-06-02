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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

/** Storage API DynamicDestinations used when the input is a Beam Row. */
class StorageApiDynamicDestinationsBeamRow<T, DestinationT extends @NonNull Object>
    extends StorageApiDynamicDestinations<T, DestinationT> {
  private final Schema schema;
  private final SerializableFunction<T, Row> toRow;

  StorageApiDynamicDestinationsBeamRow(
      DynamicDestinations<T, DestinationT> inner,
      Schema schema,
      SerializableFunction<T, Row> toRow) {
    super(inner);
    this.schema = schema;
    this.toRow = toRow;
  }

  @Override
  public MessageConverter<T> getMessageConverter(
      DestinationT destination, DatasetService datasetService) throws Exception {
    return new MessageConverter<T>() {
      final Descriptor descriptor;
      final long descriptorHash;

      {
        descriptor = BeamRowToStorageApiProto.getDescriptorFromSchema(schema);
        descriptorHash = BigQueryUtils.hashSchemaDescriptorDeterministic(descriptor);
      }

      @Override
      public DescriptorWrapper getSchemaDescriptor() {
        return new DescriptorWrapper(descriptor, descriptorHash);
      }

      @Override
      public void refreshSchema(long expectedHash) {}

      @Override
      public StorageApiWritePayload toMessage(T element) {
        Message msg = BeamRowToStorageApiProto.messageFromBeamRow(descriptor, toRow.apply(element));
        return new AutoValue_StorageApiWritePayload(msg.toByteArray(), descriptorHash);
      }

      @Override
      public TableRow toTableRow(T element) {
        return BigQueryUtils.toTableRow(toRow.apply(element));
      }
    };
  }
}
