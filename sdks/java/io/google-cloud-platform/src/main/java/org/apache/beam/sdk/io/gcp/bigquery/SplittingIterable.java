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

import com.google.protobuf.Descriptors;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.ThrowingSupplier;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.PeekingIterator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Takes in an iterable and batches the results into multiple ProtoRows objects. The splitSize
 * parameter controls how many rows are batched into a single ProtoRows object before we move on to
 * the next one.
 */
class SplittingIterable implements Iterable<AppendRowsPacket> {
  private final Iterable<StoragePayloadWithDeadline> underlying;
  private final long splitSize;

  private final Consumer<TimestampedValue<BigQueryStorageApiInsertError>> failedRowsConsumer;
  private final ThrowingSupplier<byte[]> getCurrentTableSchemaHash;
  private final ThrowingSupplier<Descriptors.Descriptor> getCurrentTableSchemaDescriptor;
  private final Instant elementTimestamp;
  private final Supplier<AppendClientInfo> appendClientSupplier;
  private final SchemaChangeDetectorHelper schemaChangeDetectorHelper;

  public SplittingIterable(
      Iterable<StoragePayloadWithDeadline> underlying,
      long splitSize,
      Consumer<TimestampedValue<BigQueryStorageApiInsertError>> failedRowsConsumer,
      ThrowingSupplier<byte[]> getCurrentTableSchemaHash,
      ThrowingSupplier<Descriptors.Descriptor> getCurrentTableSchemaDescriptor,
      Instant elementTimestamp,
      Supplier<AppendClientInfo> appendClientSupplier,
      SchemaChangeDetectorHelper schemaChangeDetectorHelper) {
    this.underlying = underlying;
    this.splitSize = splitSize;
    this.failedRowsConsumer = failedRowsConsumer;
    this.getCurrentTableSchemaHash = getCurrentTableSchemaHash;
    this.getCurrentTableSchemaDescriptor = getCurrentTableSchemaDescriptor;
    this.elementTimestamp = elementTimestamp;
    this.appendClientSupplier = appendClientSupplier;
    this.schemaChangeDetectorHelper = schemaChangeDetectorHelper;
  }

  @Override
  public Iterator<AppendRowsPacket> iterator() {
    return new Iterator<AppendRowsPacket>() {
      final PeekingIterator<StoragePayloadWithDeadline> underlyingIterator =
          Iterators.peekingIterator(underlying.iterator());
      @Nullable AppendRowsPacket cachedNext = null;

      @Override
      public boolean hasNext() {
        if (cachedNext == null && underlyingIterator.hasNext()) {
          cachedNext = calculateNext();
        }
        return cachedNext != null;
      }

      @Override
      public AppendRowsPacket next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        AppendRowsPacket result = Preconditions.checkStateNotNull(cachedNext);
        cachedNext = null;
        return result;
      }

      public @Nullable AppendRowsPacket calculateNext() {
        AppendRowsPacket value =
            AppendRowsPacket.fromStorageApiWritePayload(
                underlyingIterator,
                splitSize,
                schemaChangeDetectorHelper,
                elementTimestamp,
                appendClientSupplier,
                failedRowsConsumer,
                getCurrentTableSchemaHash,
                getCurrentTableSchemaDescriptor);
        return value.getProtoRows().getSerializedRowsCount() == 0 ? null : value;
      }
    };
  }
}
