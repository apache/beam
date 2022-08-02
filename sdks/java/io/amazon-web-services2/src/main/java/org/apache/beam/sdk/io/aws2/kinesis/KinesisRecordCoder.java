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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.joda.time.Instant;

/** A {@link Coder} for {@link KinesisRecord}. */
class KinesisRecordCoder extends AtomicCoder<KinesisRecord> {

  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
  private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();
  private static final InstantCoder INSTANT_CODER = InstantCoder.of();
  private static final VarLongCoder VAR_LONG_CODER = VarLongCoder.of();

  public static KinesisRecordCoder of() {
    return new KinesisRecordCoder();
  }

  @Override
  public void encode(KinesisRecord value, OutputStream outStream) throws IOException {
    BYTE_ARRAY_CODER.encode(value.getDataAsBytes(), outStream);
    STRING_CODER.encode(value.getSequenceNumber(), outStream);
    STRING_CODER.encode(value.getPartitionKey(), outStream);
    INSTANT_CODER.encode(value.getApproximateArrivalTimestamp(), outStream);
    VAR_LONG_CODER.encode(value.getSubSequenceNumber(), outStream);
    INSTANT_CODER.encode(value.getReadTime(), outStream);
    STRING_CODER.encode(value.getStreamName(), outStream);
    STRING_CODER.encode(value.getShardId(), outStream);
  }

  @Override
  public KinesisRecord decode(InputStream inStream) throws IOException {
    ByteBuffer data = ByteBuffer.wrap(BYTE_ARRAY_CODER.decode(inStream));
    String sequenceNumber = STRING_CODER.decode(inStream);
    String partitionKey = STRING_CODER.decode(inStream);
    Instant approximateArrivalTimestamp = INSTANT_CODER.decode(inStream);
    long subSequenceNumber = VAR_LONG_CODER.decode(inStream);
    Instant readTimestamp = INSTANT_CODER.decode(inStream);
    String streamName = STRING_CODER.decode(inStream);
    String shardId = STRING_CODER.decode(inStream);
    return new KinesisRecord(
        data,
        sequenceNumber,
        subSequenceNumber,
        partitionKey,
        approximateArrivalTimestamp,
        readTimestamp,
        streamName,
        shardId);
  }
}
