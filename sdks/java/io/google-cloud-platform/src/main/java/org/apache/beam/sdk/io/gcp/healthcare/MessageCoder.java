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
package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1alpha2.model.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

public class MessageCoder extends CustomCoder<Message> {
  MessageCoder() {}

  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());

  @Override
  public void encode(Message value, OutputStream outStream) throws CoderException, IOException {
    STRING_CODER.encode(value.getName(), outStream);
    STRING_CODER.encode(value.getMessageType(), outStream);
    STRING_CODER.encode(value.getCreateTime(), outStream);
    STRING_CODER.encode(value.getSendTime(), outStream);
    STRING_CODER.encode(value.getData(), outStream);
  }

  @Override
  public Message decode(InputStream inStream) throws CoderException, IOException {
    Message msg = new Message();
    msg.setName(STRING_CODER.decode(inStream));
    msg.setMessageType(STRING_CODER.decode(inStream));
    msg.setCreateTime(STRING_CODER.decode(inStream));
    msg.setSendTime(STRING_CODER.decode(inStream));
    msg.setData(STRING_CODER.decode(inStream));
    return msg;
  }
}
