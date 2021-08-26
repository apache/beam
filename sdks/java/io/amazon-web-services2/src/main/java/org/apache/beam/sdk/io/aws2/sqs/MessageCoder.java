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
package org.apache.beam.sdk.io.aws2.sqs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import software.amazon.awssdk.services.sqs.model.Message;

/** Custom Coder for handling SendMessageRequest for using in Write. */
public class MessageCoder extends AtomicCoder<Message> implements Serializable {
  private static final MessageCoder INSTANCE = new MessageCoder();

  private MessageCoder() {}

  static MessageCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(Message value, OutputStream outStream) throws IOException {
    StringUtf8Coder.of().encode(value.messageId(), outStream);
    StringUtf8Coder.of().encode(value.body(), outStream);
  }

  @Override
  public Message decode(InputStream inStream) throws IOException {
    final String messageId = StringUtf8Coder.of().decode(inStream);
    final String body = StringUtf8Coder.of().decode(inStream);
    return Message.builder().messageId(messageId).body(body).build();
  }
}
