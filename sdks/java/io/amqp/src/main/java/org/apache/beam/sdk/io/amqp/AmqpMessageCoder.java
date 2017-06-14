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

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.commons.io.IOUtils;
import org.apache.qpid.proton.message.Message;

/**
 * A coder for AMQP message.
 */
public class AmqpMessageCoder extends CustomCoder<Message> {

  private final StringUtf8Coder stringCoder = StringUtf8Coder.of();

  static AmqpMessageCoder of() {
    return new AmqpMessageCoder();
  }

  @Override
  public void encode(Message value, OutputStream outStream) throws CoderException, IOException {
    byte[] data = new byte[16384];
    value.encode(data, 0, data.length);
    outStream.write(data);
  }

  @Override
  public Message decode(InputStream inStream) throws CoderException, IOException {
    Message message = Message.Factory.create();
    byte[] data = IOUtils.toByteArray(inStream);
    message.decode(data, 0, data.length);
    return message;
  }

}
