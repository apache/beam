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

import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiDynamicDestinations.DescriptorWrapper;

/**
 * Takes in an iterable and batches the results into multiple ProtoRows objects. The splitSize
 * parameter controls how many rows are batched into a single ProtoRows object before we move on to
 * the next one.
 */
class SplittingIterable implements Iterable<ProtoRows> {
  private final Iterable<StorageApiWritePayload> underlying;
  private final long splitSize;
  private final Function<Long, DescriptorWrapper> updateSchema;
  private DescriptorWrapper currentDescriptor;

  public SplittingIterable(
      Iterable<StorageApiWritePayload> underlying,
      long splitSize,
      DescriptorWrapper currentDescriptor,
      Function<Long, DescriptorWrapper> updateSchema) {
    this.underlying = underlying;
    this.splitSize = splitSize;
    this.updateSchema = updateSchema;
    this.currentDescriptor = currentDescriptor;
  }

  @Override
  public Iterator<ProtoRows> iterator() {
    return new Iterator<ProtoRows>() {
      final Iterator<StorageApiWritePayload> underlyingIterator = underlying.iterator();

      @Override
      public boolean hasNext() {
        return underlyingIterator.hasNext();
      }

      @Override
      public ProtoRows next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        ProtoRows.Builder inserts = ProtoRows.newBuilder();
        long bytesSize = 0;
        while (underlyingIterator.hasNext()) {
          StorageApiWritePayload payload = underlyingIterator.next();
          if (payload.getSchemaHash() != currentDescriptor.hash) {
            // Schema doesn't match. Try and get an updated schema hash (from the base table).
            currentDescriptor = updateSchema.apply(payload.getSchemaHash());
            // Validate that the record can now be parsed.
            try {
              DynamicMessage msg =
                  DynamicMessage.parseFrom(currentDescriptor.descriptor, payload.getPayload());
              if (msg.getUnknownFields() != null && !msg.getUnknownFields().asMap().isEmpty()) {
                throw new RuntimeException(
                    "Record schema does not match table. Unknown fields: "
                        + msg.getUnknownFields());
              }
            } catch (InvalidProtocolBufferException e) {
              throw new RuntimeException(e);
            }
          }
          ByteString byteString = ByteString.copyFrom(payload.getPayload());
          inserts.addSerializedRows(byteString);
          bytesSize += byteString.size();
          if (bytesSize > splitSize) {
            break;
          }
        }
        return inserts.build();
      }
    };
  }
}
