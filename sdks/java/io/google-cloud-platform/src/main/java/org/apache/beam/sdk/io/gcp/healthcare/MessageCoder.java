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

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1alpha2.model.Message;
import com.google.api.services.healthcare.v1alpha2.model.ParsedData;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

public class MessageCoder extends CustomCoder<Message> {
  MessageCoder() {}

  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  private static final NullableCoder<Map<String, String>> MAP_CODER =
      NullableCoder.of(MapCoder.of(STRING_CODER, STRING_CODER));

  @Override
  public void encode(Message value, OutputStream outStream) throws CoderException, IOException {
    STRING_CODER.encode(value.getName(), outStream);
    STRING_CODER.encode(value.getMessageType(), outStream);
    STRING_CODER.encode(value.getCreateTime(), outStream);
    STRING_CODER.encode(value.getSendTime(), outStream);
    STRING_CODER.encode(value.getData(), outStream);
    STRING_CODER.encode(value.getSendFacility(), outStream);
    MAP_CODER.encode(value.getLabels(), outStream);
    if (value.getParsedData() != null) {
      STRING_CODER.encode(value.getParsedData().toString(), outStream);
    } else {
      STRING_CODER.encode(null, outStream);
    }
  }

  @Override
  public Message decode(InputStream inStream) throws CoderException, IOException {
    Message msg = new Message();
    msg.setName(STRING_CODER.decode(inStream));
    msg.setMessageType(STRING_CODER.decode(inStream));
    msg.setCreateTime(STRING_CODER.decode(inStream));
    msg.setSendTime(STRING_CODER.decode(inStream));
    msg.setData(STRING_CODER.decode(inStream));
    msg.setSendFacility(STRING_CODER.decode(inStream));
    msg.setLabels(MAP_CODER.decode(inStream));
    String parsedDataStr = STRING_CODER.decode(inStream);
    if (parsedDataStr != null) {
      // TODO: find better workaround to JsonFactory not serializable.
      JsonFactory jsonFactory = new GsonFactory();
      msg.setParsedData(jsonFactory.fromString(parsedDataStr, ParsedData.class));
    }
    return msg;
  }
}
