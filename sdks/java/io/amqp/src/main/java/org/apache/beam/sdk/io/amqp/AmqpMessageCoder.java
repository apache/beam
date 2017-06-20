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

import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.util.VarInt;
import org.apache.qpid.proton.message.Message;

/**
 * A coder for AMQP message.
 */
public class AmqpMessageCoder extends CustomCoder<AmqpMessage> {

  static AmqpMessageCoder of() {
    return new AmqpMessageCoder();
  }

  // private static final int[] MESSAGE_SIZES = [1 << 14 /* 16 KiB */,1 << 20 /* 1 MiB */, 1 << 26
  //    /* 64 MiB */]

  @Override
  public void encode(AmqpMessage value, OutputStream outStream) throws CoderException, IOException {
    //for (int maxMessageSize : MESSAGE_SIZES) {
      try {
        byte[] data = new byte[4096];
        int bytesWritten = value.getMessage().encode(data, 0, data.length);
        VarInt.encode(bytesWritten, outStream);
        outStream.write(data, 0, bytesWritten);
        return;
      } catch (Exception ignored) {  // <-- ProtonJ javadoc says it throws an exception if the
        // message doesn't fit into the byte[] but it doesn't state which one.
        // Try to encode into a larger byte array since the current one was too small
      }
    //}
  }

  @Override
  public AmqpMessage decode(InputStream inStream) throws CoderException, IOException {
    Message message = Message.Factory.create();
    int bytesToRead = VarInt.decodeInt(inStream);
    byte[] encodedMessage = new byte[bytesToRead];
    ByteStreams.readFully(inStream, encodedMessage);
    message.decode(encodedMessage, 0, encodedMessage.length);
    return new AmqpMessage(message);
  }

}
