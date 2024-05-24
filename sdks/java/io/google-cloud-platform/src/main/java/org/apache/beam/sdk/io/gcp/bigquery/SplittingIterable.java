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
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.beam.sdk.io.gcp.bigquery.SplittingIterable.Value;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Takes in an iterable and batches the results into multiple ProtoRows objects. The splitSize
 * parameter controls how many rows are batched into a single ProtoRows object before we move on to
 * the next one.
 */
class SplittingIterable<ElementT> implements Iterable<Value<ElementT>> {
  @AutoValue
  abstract static class Value<ElementT> {
    abstract ProtoRows getProtoRows();

    abstract List<ElementT> getOriginalElements();

    abstract List<Instant> getTimestamps();
  }

  interface ConvertUnknownFields {
    ByteString convert(TableRow tableRow, boolean ignoreUnknownValues)
        throws TableRowToStorageApiProto.SchemaConversionException;
  }

  private final Iterable<StorageApiWritePayload<ElementT>> underlying;
  private final long splitSize;

  private final ConvertUnknownFields unknownFieldsToMessage;
  private final Function<ByteString, TableRow> protoToTableRow;
  private final BiConsumer<TimestampedValue<TableRow>, String> failedRowsConsumer;
  private final boolean autoUpdateSchema;
  private final boolean ignoreUnknownValues;

  private final Instant elementsTimestamp;

  @Nullable SerializableFunction<ElementT, TableRow> formatRecordOnFailureFunction;

  public SplittingIterable(
      Iterable<StorageApiWritePayload<ElementT>> underlying,
      long splitSize,
      ConvertUnknownFields unknownFieldsToMessage,
      Function<ByteString, TableRow> protoToTableRow,
      BiConsumer<TimestampedValue<TableRow>, String> failedRowsConsumer,
      boolean autoUpdateSchema,
      boolean ignoreUnknownValues,
      Instant elementsTimestamp,
      @Nullable SerializableFunction<ElementT, TableRow> formatRecordOnFailureFunction) {
    this.underlying = underlying;
    this.splitSize = splitSize;
    this.unknownFieldsToMessage = unknownFieldsToMessage;
    this.protoToTableRow = protoToTableRow;
    this.failedRowsConsumer = failedRowsConsumer;
    this.autoUpdateSchema = autoUpdateSchema;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.elementsTimestamp = elementsTimestamp;
    this.formatRecordOnFailureFunction = formatRecordOnFailureFunction;
  }

  @Override
  public Iterator<Value<ElementT>> iterator() {
    return new Iterator<Value<ElementT>>() {
      final Iterator<StorageApiWritePayload<ElementT>> underlyingIterator =
          underlying.iterator();

      @Override
      public boolean hasNext() {
        return underlyingIterator.hasNext();
      }

      @Override
      public Value<ElementT> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        List<Instant> timestamps = Lists.newArrayList();
        ProtoRows.Builder inserts = ProtoRows.newBuilder();
        List<ElementT> originalElements = new ArrayList<>();
        long bytesSize = 0;
        while (underlyingIterator.hasNext()) {
          StorageApiWritePayload<ElementT> payload = underlyingIterator.next();
          ByteString byteString = ByteString.copyFrom(payload.getPayload());
          if (autoUpdateSchema) {
            try {
              @Nullable TableRow unknownFields = payload.getUnknownFields();
              if (unknownFields != null && !unknownFields.isEmpty()) {
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
                  TableRow tableRow;
                  if (formatRecordOnFailureFunction != null) {
                    tableRow = formatRecordOnFailureFunction.apply(payload.originalElement());
                  } else {
                    tableRow = protoToTableRow.apply(byteString);
                  }
                  // TODO(24926, reuvenlax): We need to merge the unknown fields in! Currently we
                  // only execute this
                  // codepath when ignoreUnknownFields==true, so we should never hit this codepath.
                  // However once
                  // 24926 is fixed, we need to merge the unknownFields back into the main row
                  // before outputting to the
                  // failed-rows consumer.
                  Instant timestamp = payload.getTimestamp();
                  if (timestamp == null) {
                    timestamp = elementsTimestamp;
                  }
                  failedRowsConsumer.accept(TimestampedValue.of(tableRow, timestamp), e.toString());
                  continue;
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          inserts.addSerializedRows(byteString);
          originalElements.add(payload.originalElement());
          Instant timestamp = payload.getTimestamp();
          if (timestamp == null) {
            timestamp = elementsTimestamp;
          }
          timestamps.add(timestamp);
          bytesSize += byteString.size();
          if (bytesSize > splitSize) {
            break;
          }
        }
        return new AutoValue_SplittingIterable_Value<ElementT>(
            inserts.build(), originalElements, timestamps);
      }
    };
  }
}
