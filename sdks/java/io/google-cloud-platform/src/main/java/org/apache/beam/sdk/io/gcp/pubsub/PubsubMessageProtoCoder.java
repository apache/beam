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
package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class PubsubMessageProtoCoder extends StructuredCoder<PubsubMessage> {
  private final ProtoCoder<com.google.pubsub.v1.PubsubMessage> coder;

  private PubsubMessageProtoCoder() {
    this.coder = ProtoCoder.of(com.google.pubsub.v1.PubsubMessage.class);
  }

  public static PubsubMessageProtoCoder of() {
    return new PubsubMessageProtoCoder();
  }

  @Override
  public void encode(
      PubsubMessage value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
      throws CoderException, IOException {
    com.google.pubsub.v1.PubsubMessage.Builder builder =
        com.google.pubsub.v1.PubsubMessage.newBuilder();
    builder.setData(ByteString.copyFrom(value.getPayload()));
    String messageId = value.getMessageId();
    if (messageId != null) {
      builder.setMessageId(messageId);
    }
    Map<String, String> attributeMap = value.getAttributeMap();
    if (attributeMap != null) {
      builder.putAllAttributes(attributeMap);
    }
    coder.encode(builder.build(), outStream);
  }

  @Override
  public PubsubMessage decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream)
      throws CoderException, IOException {
    com.google.pubsub.v1.PubsubMessage message = coder.decode(inStream);
    return new PubsubMessage(
        message.getData().toByteArray(), message.getAttributesMap(), message.getMessageId());
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<? extends Coder<?>> getCoderArguments() {
    return Collections.singletonList(coder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    coder.verifyDeterministic();
  }
}
