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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;

import java.io.Serializable;
import java.util.Objects;
import org.joda.time.Instant;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * Checkpoint mark for single shard in the stream. Current position in the shard is determined by
 * either:
 *
 * <ul>
 *   <li>{@link #shardIteratorType} if it is equal to {@link ShardIteratorType#LATEST} or {@link
 *       ShardIteratorType#TRIM_HORIZON}
 *   <li>combination of {@link #sequenceNumber} and {@link #subSequenceNumber} if {@link
 *       ShardIteratorType#AFTER_SEQUENCE_NUMBER} or {@link ShardIteratorType#AT_SEQUENCE_NUMBER}
 * </ul>
 *
 * This class is immutable.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class ShardCheckpoint implements Serializable {
  /**
   * Extracted from org.apache.beam:beam-sdks-java-io-amazon-web-services2:2.46.0.
   *
   * <pre>{@code
   * serialver -classpath "..<dependencies dir>/*" \
   *     org.apache.beam.sdk.io.aws2.kinesis.ShardCheckpoint
   * }</pre>
   */
  private static final long serialVersionUID = 103536540299998471L;

  private final String streamName;
  private final String shardId;
  private final String sequenceNumber;
  private final ShardIteratorType shardIteratorType;
  private final Long subSequenceNumber;
  private final Instant timestamp;

  public ShardCheckpoint(String streamName, String shardId, StartingPoint startingPoint) {
    this(
        streamName,
        shardId,
        ShardIteratorType.fromValue(startingPoint.getPositionName()),
        startingPoint.getTimestamp());
  }

  public ShardCheckpoint(
      String streamName, String shardId, ShardIteratorType shardIteratorType, Instant timestamp) {
    this(streamName, shardId, shardIteratorType, null, null, timestamp);
  }

  public ShardCheckpoint(
      String streamName,
      String shardId,
      ShardIteratorType shardIteratorType,
      String sequenceNumber,
      Long subSequenceNumber) {
    this(streamName, shardId, shardIteratorType, sequenceNumber, subSequenceNumber, null);
  }

  private ShardCheckpoint(
      String streamName,
      String shardId,
      ShardIteratorType shardIteratorType,
      String sequenceNumber,
      Long subSequenceNumber,
      Instant timestamp) {
    this.shardIteratorType = checkNotNull(shardIteratorType, "shardIteratorType");
    this.streamName = checkNotNull(streamName, "streamName");
    this.shardId = checkNotNull(shardId, "shardId");
    if (shardIteratorType == AT_SEQUENCE_NUMBER || shardIteratorType == AFTER_SEQUENCE_NUMBER) {
      checkNotNull(
          sequenceNumber,
          "You must provide sequence number for AT_SEQUENCE_NUMBER" + " or AFTER_SEQUENCE_NUMBER");
    } else {
      checkArgument(
          sequenceNumber == null,
          "Sequence number must be null for LATEST, TRIM_HORIZON or AT_TIMESTAMP");
    }
    if (shardIteratorType == AT_TIMESTAMP) {
      checkNotNull(timestamp, "You must provide timestamp for AT_TIMESTAMP");
    } else {
      checkArgument(
          timestamp == null, "Timestamp must be null for an iterator type other than AT_TIMESTAMP");
    }

    this.subSequenceNumber = subSequenceNumber;
    this.sequenceNumber = sequenceNumber;
    this.timestamp = timestamp;
  }

  /**
   * Used to compare {@link ShardCheckpoint} object to {@link KinesisRecord}. Depending on the
   * underlying shardIteratorType, it will either compare the timestamp or the {@link
   * ExtendedSequenceNumber}.
   *
   * @param other
   * @return if current checkpoint mark points before or at given {@link ExtendedSequenceNumber}
   */
  public boolean isBeforeOrAt(KinesisRecord other) {
    if (shardIteratorType == AT_TIMESTAMP) {
      return timestamp.compareTo(other.getApproximateArrivalTimestamp()) <= 0;
    }
    int result = extendedSequenceNumber().compareTo(other.getExtendedSequenceNumber());
    if (result == 0) {
      return shardIteratorType == AT_SEQUENCE_NUMBER;
    }
    return result < 0;
  }

  private ExtendedSequenceNumber extendedSequenceNumber() {
    String fullSequenceNumber = sequenceNumber;
    if (fullSequenceNumber == null) {
      fullSequenceNumber = shardIteratorType.toString();
    }
    return new ExtendedSequenceNumber(fullSequenceNumber, subSequenceNumber);
  }

  @Override
  public String toString() {
    return String.format(
        "Checkpoint %s for stream %s, shard %s: %s %s",
        shardIteratorType, streamName, shardId, sequenceNumber, subSequenceNumber);
  }

  /**
   * Returns new iterator using the ack-ed checkpoint.
   *
   * <p>Note that {@link #checkpointIsInTheMiddleOfAUserRecord} is always true for cases when at
   * least one record was ack-ed. {@link #sequenceNumber} will be never null, and only {@link
   * ShardIteratorType#AFTER_SEQUENCE_NUMBER} is checkpoint-ed.
   *
   * <p>This means that iterator will always deliver some redundant records in the beginning, which
   * are for {@link RecordFilter} to deal with.
   *
   * @param kinesisClient
   * @return
   * @throws TransientKinesisException
   */
  public String getShardIterator(SimplifiedKinesisClient kinesisClient)
      throws TransientKinesisException {
    if (checkpointIsInTheMiddleOfAUserRecord()) {
      return kinesisClient.getShardIterator(
          streamName, shardId, AT_SEQUENCE_NUMBER, sequenceNumber, null);
    }
    return kinesisClient.getShardIterator(
        streamName, shardId, shardIteratorType, sequenceNumber, timestamp);
  }

  private boolean checkpointIsInTheMiddleOfAUserRecord() {
    return shardIteratorType == AFTER_SEQUENCE_NUMBER && subSequenceNumber != null;
  }

  /**
   * Used to advance checkpoint mark to position after given {@link Record}.
   *
   * <p>Note that only {@link ShardIteratorType#AFTER_SEQUENCE_NUMBER} is persisted, as soon as
   * {@link #moveAfter(KinesisRecord)} called at least once.
   *
   * @param record
   * @return new checkpoint object pointing directly after given {@link Record}
   */
  public ShardCheckpoint moveAfter(KinesisRecord record) {
    return new ShardCheckpoint(
        streamName,
        shardId,
        AFTER_SEQUENCE_NUMBER,
        record.getSequenceNumber(),
        record.getSubSequenceNumber());
  }

  public String getStreamName() {
    return streamName;
  }

  public String getShardId() {
    return shardId;
  }

  /**
   * Converts stored checkpoint into start position.
   *
   * <p>It follows the semantics of {@link #getShardIterator(SimplifiedKinesisClient)} which
   * effectively forces {@link ShardIteratorType#AT_SEQUENCE_NUMBER} for fetching first batch, all
   * the time.
   *
   * <p>{@link #moveAfter(KinesisRecord)} never stores {@link ShardIteratorType#AT_SEQUENCE_NUMBER}
   * and, instead, relies on {@link RecordFilter} to drop first redundant de-aggregated records or
   * entire batch of de-aggregated or "normal" records.
   */
  StartingPosition toEFOStartingPosition() {
    StartingPosition.Builder builder = StartingPosition.builder().type(shardIteratorType);
    switch (shardIteratorType) {
      case AT_TIMESTAMP:
        return builder.timestamp(TimeUtil.toJava(checkNotNull(timestamp))).build();
      case AT_SEQUENCE_NUMBER:
      case AFTER_SEQUENCE_NUMBER:
        return StartingPosition.builder()
            .type(AT_SEQUENCE_NUMBER)
            .sequenceNumber(checkNotNull(sequenceNumber))
            .build();

      default:
        return builder.build();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShardCheckpoint that = (ShardCheckpoint) o;
    return streamName.equals(that.streamName)
        && shardId.equals(that.shardId)
        && Objects.equals(sequenceNumber, that.sequenceNumber)
        && shardIteratorType == that.shardIteratorType
        && Objects.equals(subSequenceNumber, that.subSequenceNumber)
        && Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        streamName, shardId, sequenceNumber, shardIteratorType, subSequenceNumber, timestamp);
  }
}
