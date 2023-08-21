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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * A coder for PubsubMessage treating the raw bytes being decoded as the message's payload, with the
 * message id from the PubSub server.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PubsubMessageWithMessageIdCoder extends CustomCoder<PubsubMessage> {
  private static final Coder<byte[]> PAYLOAD_CODER = ByteArrayCoder.of();
  // A message's messageId cannot be null
  private static final Coder<String> MESSAGE_ID_CODER = StringUtf8Coder.of();

  public static PubsubMessageWithMessageIdCoder of() {
    return new PubsubMessageWithMessageIdCoder();
  }

  @Override
  public void encode(PubsubMessage value, OutputStream outStream) throws IOException {
    PAYLOAD_CODER.encode(value.getPayload(), outStream);
    MESSAGE_ID_CODER.encode(value.getMessageId(), outStream);
  }

  @Override
  public PubsubMessage decode(InputStream inStream) throws IOException {
    byte[] payload = PAYLOAD_CODER.decode(inStream);
    String messageId = MESSAGE_ID_CODER.decode(inStream);
    return new PubsubMessage(payload, ImmutableMap.of(), messageId);
  }
}
