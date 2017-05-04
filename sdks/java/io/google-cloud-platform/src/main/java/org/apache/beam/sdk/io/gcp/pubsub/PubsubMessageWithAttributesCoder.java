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
import java.util.Map;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A coder for PubsubMessage including attributes. */
public class PubsubMessageWithAttributesCoder extends CustomCoder<PubsubMessage> {
  private static final Coder<byte[]> PAYLOAD_CODER =
      NullableCoder.of(ByteArrayCoder.of());
  private static final Coder<Map<String, String>> ATTRIBUTES_CODER = MapCoder.of(
      StringUtf8Coder.of(), StringUtf8Coder.of());

  public static Coder<PubsubMessage> of(TypeDescriptor<PubsubMessage> ignored) {
    return of();
  }

  public static PubsubMessageWithAttributesCoder of() {
    return new PubsubMessageWithAttributesCoder();
  }

  public void encode(PubsubMessage value, OutputStream outStream, Context context)
      throws IOException {
    PAYLOAD_CODER.encode(
        value.getPayload(),
        outStream,
        context.nested());
    ATTRIBUTES_CODER.encode(value.getAttributeMap(), outStream, context);
  }

  @Override
  public PubsubMessage decode(InputStream inStream, Context context) throws IOException {
    byte[] payload = PAYLOAD_CODER.decode(inStream, context.nested());
    Map<String, String> attributes = ATTRIBUTES_CODER.decode(inStream, context);
    return new PubsubMessage(payload, attributes);
  }
}
