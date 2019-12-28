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

import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A coder for IncomingMessage. */
public class IncomingMessageCoder extends CustomCoder<IncomingMessage> {
  public static CoderProvider getCoderProvider() {
    return CoderProviders.forCoder(
        TypeDescriptor.of(IncomingMessage.class), new IncomingMessageCoder());
  }

  public static IncomingMessageCoder of() {
    return new IncomingMessageCoder();
  }

  @Override
  public void encode(IncomingMessage value, OutputStream outStream) throws IOException {
    ProtoCoder.of(PubsubMessage.class).encode(value.message(), outStream);
    VarLongCoder.of().encode(value.timestampMsSinceEpoch(), outStream);
    VarLongCoder.of().encode(value.requestTimeMsSinceEpoch(), outStream);
    StringUtf8Coder.of().encode(value.ackId(), outStream);
    StringUtf8Coder.of().encode(value.recordId(), outStream);
  }

  @Override
  public IncomingMessage decode(InputStream inStream) throws IOException {
    return IncomingMessage.of(
        ProtoCoder.of(PubsubMessage.class).decode(inStream),
        VarLongCoder.of().decode(inStream),
        VarLongCoder.of().decode(inStream),
        StringUtf8Coder.of().decode(inStream),
        StringUtf8Coder.of().decode(inStream));
  }
}
