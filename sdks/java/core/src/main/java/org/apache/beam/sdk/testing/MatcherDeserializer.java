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
package org.apache.beam.sdk.testing;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;

/** MatcherDeserializer is used with Jackson to enable deserialization of SerializableMatchers. */
class MatcherDeserializer extends JsonDeserializer<SerializableMatcher<?>> {
  @Override
  public SerializableMatcher<?> deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JsonProcessingException {
    ObjectNode node = jsonParser.readValueAsTree();
    String matcher = node.get("matcher").asText();
    byte[] in = BaseEncoding.base64().decode(matcher);
    return (SerializableMatcher<?>)
        SerializableUtils.deserializeFromByteArray(in, "SerializableMatcher");
  }
}
