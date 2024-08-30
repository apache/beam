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
package org.apache.beam.sdk.io.aws2.kinesis;

import static java.nio.charset.StandardCharsets.UTF_8;
import static software.amazon.awssdk.core.SdkBytes.fromByteArrayUnsafe;
import static software.amazon.kinesis.retrieval.kpl.Messages.Record;

import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.kpl.Messages.AggregatedRecord;

/**
 * Record aggregator compatible with the record (de)aggregation of the Kinesis Producer Library
 * (KPL) and Kinesis Client Library (KCL).
 *
 * <p>https://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-concepts.html#kinesis-kpl-concepts-aggretation
 */
@NotThreadSafe
@Internal
class RecordsAggregator {
  private static final byte[] MAGIC_BYTES = AggregatorUtil.AGGREGATED_RECORD_MAGIC;
  private static final int DIGEST_SIZE = 16;
  @VisibleForTesting static final int BASE_OVERHEAD = MAGIC_BYTES.length + DIGEST_SIZE;

  private final int maxAggregatedBytes;
  private final AggregatedRecord.Builder aggBuilder;
  private final Map<String, Integer> partitionKeys;
  private final Map<String, Integer> explicitHashKeys;
  private int sizeBytes;
  private Instant timeout;

  RecordsAggregator(int maxAggregatedBytes, Instant timeout) {
    this.maxAggregatedBytes = maxAggregatedBytes;
    this.aggBuilder = AggregatedRecord.newBuilder();
    this.partitionKeys = new TreeMap<>();
    this.explicitHashKeys = new TreeMap<>();
    this.sizeBytes = BASE_OVERHEAD;
    this.timeout = timeout;
  }

  int getRecordsCount() {
    return aggBuilder.getRecordsCount();
  }

  boolean addRecord(String partitionKey, @Nullable String explicitHashKey, byte[] record) {
    int recordSize = sizeIncrement(partitionKey, explicitHashKey, record);
    if (sizeBytes + recordSize > maxAggregatedBytes) {
      return false;
    }

    ByteString data = record != null ? ByteString.copyFrom(record) : ByteString.EMPTY;
    Record.Builder recordBuilder = Record.newBuilder().setData(data);

    recordBuilder.setPartitionKeyIndex(
        partitionKeys.computeIfAbsent(
            partitionKey,
            pk -> aggBuilder.addPartitionKeyTable(pk).getPartitionKeyTableCount() - 1));

    if (explicitHashKey != null) {
      recordBuilder.setExplicitHashKeyIndex(
          explicitHashKeys.computeIfAbsent(
              explicitHashKey,
              ehk -> aggBuilder.addExplicitHashKeyTable(ehk).getExplicitHashKeyTableCount() - 1));
    }

    aggBuilder.addRecords(recordBuilder.build());
    sizeBytes += recordSize;
    return true;
  }

  boolean hasCapacity() {
    if (aggBuilder.getRecordsCount() == 0) {
      return true;
    }
    int avgSize = (sizeBytes - BASE_OVERHEAD) / aggBuilder.getRecordsCount();
    return sizeBytes + avgSize <= maxAggregatedBytes;
  }

  PutRecordsRequestEntry get() {
    PutRecordsRequestEntry.Builder entryBuilder =
        PutRecordsRequestEntry.builder().data(fromByteArrayUnsafe(toBytes()));
    if (aggBuilder.getExplicitHashKeyTableCount() > 0) {
      entryBuilder.partitionKey("a").explicitHashKey(aggBuilder.getExplicitHashKeyTable(0));
    } else {
      entryBuilder.partitionKey(aggBuilder.getPartitionKeyTable(0));
    }
    return entryBuilder.build();
  }

  PutRecordsRequestEntry getAndReset(Instant nextTimeout) {
    PutRecordsRequestEntry entry = get();
    reset(nextTimeout);
    return entry;
  }

  Instant timeout() {
    return timeout;
  }

  private void reset(Instant nextTimeout) {
    aggBuilder.clearRecords();
    aggBuilder.clearPartitionKeyTable();
    aggBuilder.clearExplicitHashKeyTable();
    partitionKeys.clear();
    explicitHashKeys.clear();
    sizeBytes = BASE_OVERHEAD;
    timeout = nextTimeout;
  }

  private static void sizeIncrementOfKey(int[] size, String key, Map<String, Integer> keyTable) {
    int keyLength = key.getBytes(UTF_8).length;
    Integer idx = keyTable.get(key);
    if (idx == null) {
      size[0] += 1 + VarInt.getLength(keyLength) + keyLength; // encoding of key in key table
    }
    size[1] += 1 + VarInt.getLength(idx != null ? idx : keyTable.size()); // key reference in record
  }

  @VisibleForTesting
  protected int sizeIncrement(String partitionKey, @Nullable String hashKey, byte[] record) {
    final int[] size = {0, 0}; // wrapper & record
    sizeIncrementOfKey(size, partitionKey, partitionKeys); // partition key encoding
    if (hashKey != null) {
      sizeIncrementOfKey(size, hashKey, explicitHashKeys); // optional hash key encoding
    }
    if (record != null) {
      size[1] += 1 + VarInt.getLength(record.length) + record.length; // record encoding
    }
    // wrapper with partition / hash key table + record with partition / hash key reference
    return size[0] + 1 + VarInt.getLength(size[1]) + size[1];
  }

  @VisibleForTesting
  protected int getSizeBytes() {
    return sizeBytes;
  }

  @VisibleForTesting
  protected byte[] toBytes() {
    try {
      MessageDigest md5 = MessageDigest.getInstance("md5");
      byte[] body = aggBuilder.build().toByteArray();
      byte[] digest = md5.digest(body);
      ByteBuffer buffer = ByteBuffer.allocate(MAGIC_BYTES.length + body.length + digest.length);
      return buffer.put(MAGIC_BYTES).put(body).put(digest).array();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 not available", e);
    }
  }
}
