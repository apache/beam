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
package org.apache.beam.sdk.io.mqtt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** A {@link Coder} for {@link MqttRecord}. */
public class MqttRecordCoder extends AtomicCoder<MqttRecord> {
  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
  private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

  public static MqttRecordCoder of() {
    return new MqttRecordCoder();
  }

  @Override
  public void encode(MqttRecord value, OutputStream outStream) throws CoderException, IOException {
    STRING_CODER.encode(value.getTopic(), outStream);
    BYTE_ARRAY_CODER.encode(value.getPayload(), outStream);
  }

  @Override
  public MqttRecord decode(InputStream inStream) throws CoderException, IOException {
    String topic = STRING_CODER.decode(inStream);
    byte[] payload = BYTE_ARRAY_CODER.decode(inStream);
    return new MqttRecord(topic, payload);
  }
}
