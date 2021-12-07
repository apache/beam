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
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.time.Duration;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;

@SuppressWarnings({"nullness"})
public class StorageApiDynamicDestinationsTableRow<T, DestinationT>
    extends StorageApiDynamicDestinations<T, DestinationT> {
  private final SerializableFunction<T, TableRow> formatFunction;

  // TODO: Make static! Or at least optimize the constant schema case.
  private final Cache<DestinationT, Descriptor> destinationDescriptorCache =
      CacheBuilder.newBuilder().expireAfterAccess(Duration.ofMinutes(15)).build();

  StorageApiDynamicDestinationsTableRow(
      DynamicDestinations<T, DestinationT> inner,
      SerializableFunction<T, TableRow> formatFunction) {
    super(inner);
    this.formatFunction = formatFunction;
  }

  @Override
  public MessageConverter<T> getMessageConverter(DestinationT destination) throws Exception {
    final TableSchema tableSchema = getSchema(destination);
    if (tableSchema == null) {
      throw new RuntimeException(
          "Schema must be set when writing TableRows using Storage API. Use "
              + "BigQueryIO.Write.withSchema to set the schema.");
    }
    return new MessageConverter<T>() {
      Descriptor descriptor =
          destinationDescriptorCache.get(
              destination,
              () -> TableRowToStorageApiProto.getDescriptorFromTableSchema(tableSchema));

      @Override
      public Descriptor getSchemaDescriptor() {
        return descriptor;
      }

      @Override
      public Message toMessage(T element) {
        return TableRowToStorageApiProto.messageFromTableRow(
            descriptor, formatFunction.apply(element));
      }
    };
  }
}
