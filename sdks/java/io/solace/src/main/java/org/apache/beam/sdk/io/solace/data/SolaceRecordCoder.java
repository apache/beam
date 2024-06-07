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
package org.apache.beam.sdk.io.solace.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.solace.data.Solace.Destination;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Custom coder for the default Solace {@link Record}
 *
 * <p>A custom coder is required to update a Dataflow job. Using a coder generated with the
 * `@DefaultSchema` annotation doesn't create an update-compatible coders.
 */
public class SolaceRecordCoder extends CustomCoder<Record> {
  private static final Coder<byte[]> BYTE_CODER = ByteArrayCoder.of();

  private static final NullableCoder<Long> LONG_CODER = NullableCoder.of(VarLongCoder.of());
  private static final NullableCoder<Integer> INTEGER_CODER = NullableCoder.of(VarIntCoder.of());
  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  private static final NullableCoder<Boolean> BOOLEAN_CODER = NullableCoder.of(BooleanCoder.of());

  public static SolaceRecordCoder of() {
    return new SolaceRecordCoder();
  }

  @Override
  public void encode(Record value, @NonNull OutputStream outStream) throws IOException {
    STRING_CODER.encode(value.getMessageId(), outStream);
    STRING_CODER.encode(value.getReplicationGroupMessageId(), outStream);
    BYTE_CODER.encode(value.getPayload(), outStream);
    Destination destination = value.getDestination();
    String destinationName = destination == null ? null : destination.getName();
    String typeName = destination == null ? null : destination.getType().toString();
    STRING_CODER.encode(destinationName, outStream);
    STRING_CODER.encode(typeName, outStream);
    LONG_CODER.encode(value.getExpiration(), outStream);
    INTEGER_CODER.encode(value.getPriority(), outStream);
    BOOLEAN_CODER.encode(value.getRedelivered(), outStream);
    STRING_CODER.encode(value.getReplyTo(), outStream);
    LONG_CODER.encode(value.getReceiveTimestamp(), outStream);
    LONG_CODER.encode(value.getSenderTimestamp(), outStream);
    LONG_CODER.encode(value.getSequenceNumber(), outStream);
    LONG_CODER.encode(value.getTimeToLive(), outStream);
    BYTE_CODER.encode(value.getAttachmentBytes(), outStream);
  }

  @Override
  public Record decode(InputStream inStream) throws IOException {
    Record.Builder builder =
        Record.builder()
            .setMessageId(STRING_CODER.decode(inStream))
            .setReplicationGroupMessageId(STRING_CODER.decode(inStream))
            .setPayload(BYTE_CODER.decode(inStream));

    String destinationName = STRING_CODER.decode(inStream);
    String destinationType = STRING_CODER.decode(inStream);
    if (destinationName != null) {
      builder.setDestination(
          Destination.builder()
              .setName(destinationName)
              .setType(
                  Objects.equals(destinationType, "QUEUE")
                      ? Solace.DestinationType.QUEUE
                      : Solace.DestinationType.TOPIC)
              .build());
    }
    return builder
        .setExpiration(LONG_CODER.decode(inStream))
        .setPriority(INTEGER_CODER.decode(inStream))
        .setRedelivered(BOOLEAN_CODER.decode(inStream))
        .setReplyTo(STRING_CODER.decode(inStream))
        .setReceiveTimestamp(LONG_CODER.decode(inStream))
        .setSenderTimestamp(LONG_CODER.decode(inStream))
        .setSequenceNumber(LONG_CODER.decode(inStream))
        .setTimeToLive(LONG_CODER.decode(inStream))
        .setAttachmentBytes(BYTE_CODER.decode(inStream))
        .build();
  }
}
