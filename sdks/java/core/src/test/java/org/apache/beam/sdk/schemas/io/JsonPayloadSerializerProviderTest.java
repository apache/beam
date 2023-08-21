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
package org.apache.beam.sdk.schemas.io;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.JsonPayloadSerializerProvider;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JsonPayloadSerializerProviderTest {
  private static final Schema SCHEMA =
      Schema.builder().addInt64Field("abc").addStringField("xyz").build();
  private static final String SERIALIZED = "{ \"abc\": 3, \"xyz\": \"qqq\" }";
  private static final Row DESERIALIZED =
      Row.withSchema(SCHEMA).withFieldValue("abc", 3L).withFieldValue("xyz", "qqq").build();

  private final JsonPayloadSerializerProvider provider = new JsonPayloadSerializerProvider();

  @Test
  public void serialize() throws Exception {
    byte[] bytes = provider.getSerializer(SCHEMA, ImmutableMap.of()).serialize(DESERIALIZED);
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> result = mapper.readValue(bytes, Map.class);
    assertEquals(3, result.get("abc"));
    assertEquals("qqq", result.get("xyz"));
  }

  @Test
  public void deserialize() {
    Row row =
        provider
            .getSerializer(SCHEMA, ImmutableMap.of())
            .deserialize(SERIALIZED.getBytes(StandardCharsets.UTF_8));
    assertEquals(DESERIALIZED, row);
  }
}
