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
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class MqttMessageCoder extends StructuredCoder<MqttMessage> {

  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private static final ByteArrayCoder byteArrayCoder = ByteArrayCoder.of();
  private static final InstantCoder instantCoder = InstantCoder.of();

  public static MqttMessageCoder of() {
    return new MqttMessageCoder();
  }

  @Override
  public void encode(MqttMessage value, OutputStream outStream) throws IOException {
    stringCoder.encode(value.getTopic(), outStream);
    byteArrayCoder.encode(value.getPayload(), outStream);
    instantCoder.encode(value.getTimestamp(), outStream);
  }

  @Override
  public MqttMessage decode(InputStream inStream) throws IOException {
    return new MqttMessage(
        stringCoder.decode(inStream),
        byteArrayCoder.decode(inStream),
        instantCoder.decode(inStream));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {}
}
