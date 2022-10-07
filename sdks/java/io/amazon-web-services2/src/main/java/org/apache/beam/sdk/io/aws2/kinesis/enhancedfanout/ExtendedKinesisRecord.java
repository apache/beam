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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout;

import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This exists because we must ack messages without records but with continuation sequence number ->
 * we must keep them until they are taken from the pool and the pool advances the checkpoint.
 *
 * <p>Original KinesisRecord assumes its data not to be null, so it can not be used directly -> we
 * must have redundant continuationSequenceNumber and subSequenceNumber here.
 */
public class ExtendedKinesisRecord {
  private final String shardId;
  private final String continuationSequenceNumber;
  private final long subSequenceNumber;
  private final @Nullable KinesisRecord kinesisRecord;

  public ExtendedKinesisRecord(
      String shardId,
      String continuationSequenceNumber,
      long subSequenceNumber,
      @Nullable KinesisRecord kinesisRecord) {
    this.shardId = shardId;
    this.continuationSequenceNumber = continuationSequenceNumber;
    this.subSequenceNumber = subSequenceNumber;
    this.kinesisRecord = kinesisRecord;
  }

  public static ExtendedKinesisRecord fromRecord(KinesisRecord record) {
    return new ExtendedKinesisRecord(
        record.getShardId(), record.getSequenceNumber(), record.getSubSequenceNumber(), record);
  }

  public static ExtendedKinesisRecord fromEmpty(String shardId, String continuationSequenceNumber) {
    return new ExtendedKinesisRecord(shardId, continuationSequenceNumber, 0L, null);
  }

  public static ExtendedKinesisRecord fromReShard(String shardId) {
    return new ExtendedKinesisRecord(shardId, "0", 0L, null);
  }

  public static ExtendedKinesisRecord fromError(String shardId) {
    return new ExtendedKinesisRecord(shardId, "-1", 0L, null);
  }

  public String getShardId() {
    return shardId;
  }

  public String getContinuationSequenceNumber() {
    return continuationSequenceNumber;
  }

  public long getSubSequenceNumber() {
    return subSequenceNumber;
  }

  public @Nullable KinesisRecord getKinesisRecord() {
    return kinesisRecord;
  }
}
