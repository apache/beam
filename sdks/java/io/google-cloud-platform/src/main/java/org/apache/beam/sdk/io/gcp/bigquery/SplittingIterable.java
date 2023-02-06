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
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * Takes in an iterable and batches the results into multiple ProtoRows objects. The splitSize
 * parameter controls how many rows are batched into a single ProtoRows object before we move on to
 * the next one.
 */
class SplittingIterable implements Iterable<ProtoRows> {
  interface ConvertUnknownFields {
    ByteString convert(TableRow tableRow, boolean ignoreUnknownValues)
        throws TableRowToStorageApiProto.SchemaConversionException;
  }

  private final Iterable<StorageApiWritePayload> underlying;
  private final long splitSize;

  private final ConvertUnknownFields unknownFieldsToMessage;
  private final Function<ByteString, TableRow> protoToTableRow;
  private final BiConsumer<TableRow, String> failedRowsConsumer;
  private final boolean autoUpdateSchema;
  private final boolean ignoreUnknownValues;

  public SplittingIterable(
      Iterable<StorageApiWritePayload> underlying,
      long splitSize,
      ConvertUnknownFields unknownFieldsToMessage,
      Function<ByteString, TableRow> protoToTableRow,
      BiConsumer<TableRow, String> failedRowsConsumer,
      boolean autoUpdateSchema,
      boolean ignoreUnknownValues) {
    this.underlying = underlying;
    this.splitSize = splitSize;
    this.unknownFieldsToMessage = unknownFieldsToMessage;
    this.protoToTableRow = protoToTableRow;
    this.failedRowsConsumer = failedRowsConsumer;
    this.autoUpdateSchema = autoUpdateSchema;
    this.ignoreUnknownValues = ignoreUnknownValues;
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
          ByteString byteString = ByteString.copyFrom(payload.getPayload());
          if (autoUpdateSchema) {
            try {
              @Nullable TableRow unknownFields = payload.getUnknownFields();
              if (unknownFields != null) {
                // Protocol buffer serialization format supports concatenation. We serialize any new
                // "known" fields
                // into a proto and concatenate to the existing proto.
                try {
                  byteString =
                      byteString.concat(
                          unknownFieldsToMessage.convert(unknownFields, ignoreUnknownValues));
                } catch (TableRowToStorageApiProto.SchemaConversionException e) {
                  // This generally implies that ignoreUnknownValues=false and there were still
                  // unknown values here.
                  // Reconstitute the TableRow and send it to the failed-rows consumer.
                  TableRow tableRow = protoToTableRow.apply(byteString);
                  // TODO(24926, reuvenlax): We need to merge the unknown fields in! Currently we
                  // only execute this
                  // codepath when ignoreUnknownFields==true, so we should never hit this codepath.
                  // However once
                  // 24926 is fixed, we need to merge the unknownFields back into the main row
                  // before outputting to the
                  // failed-rows consumer.
                  failedRowsConsumer.accept(tableRow, e.toString());
                  continue;
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
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
