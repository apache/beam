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
package org.apache.beam.sdk.io.amqp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.qpid.proton.message.Message;

/** A coder for AMQP message. */
public class AmqpMessageCoder extends CustomCoder<Message> {

  private static final int[] MESSAGE_SIZES =
      new int[] {8 * 1024, 64 * 1024, 1 * 1024 * 1024, 64 * 1024 * 1024};

  static AmqpMessageCoder of() {
    return new AmqpMessageCoder();
  }

  @Override
  public void encode(Message value, OutputStream outStream) throws CoderException, IOException {
    for (int maxMessageSize : MESSAGE_SIZES) {
      try {
        encode(value, outStream, maxMessageSize);
        return;
      } catch (Exception e) {
        continue;
      }
    }
    throw new CoderException("Message is larger than the max size supported by the coder");
  }

  private void encode(Message value, OutputStream outStream, int messageSize)
      throws IOException, BufferOverflowException {
    byte[] data = new byte[messageSize];
    int bytesWritten = value.encode(data, 0, data.length);
    VarInt.encode(bytesWritten, outStream);
    outStream.write(data, 0, bytesWritten);
  }

  @Override
  public Message decode(InputStream inStream) throws CoderException, IOException {
    Message message = Message.Factory.create();
    int bytesToRead = VarInt.decodeInt(inStream);
    byte[] encodedMessage = new byte[bytesToRead];
    ByteStreams.readFully(inStream, encodedMessage);
    message.decode(encodedMessage, 0, encodedMessage.length);
    return message;
  }
}
