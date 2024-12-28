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
package org.apache.beam.sdk.io.rabbitmq;

import static org.junit.Assert.assertEquals;

import com.rabbitmq.client.LongString;
import com.rabbitmq.client.impl.LongStringHelper;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test of {@link RabbitMqMessage}. */
@RunWith(JUnit4.class)
public class RabbitMqMessageTest implements Serializable {

  @Test(expected = UnsupportedOperationException.class)
  public void testSerializableHeadersThrowsIfValueIsNotSerializable() {
    Map<String, Object> rawHeaders = new HashMap<>();
    Object notSerializableObject = Optional.of(new Object());
    rawHeaders.put("key1", notSerializableObject);
    RabbitMqMessage.serializableHeaders(rawHeaders);
  }

  @Test
  public void testSerializableHeadersWithLongStringValues() {
    Map<String, Object> rawHeaders = new HashMap<>();
    String key1 = "key1", key2 = "key2", value1 = "value1", value2 = "value2";
    rawHeaders.put(key1, LongStringHelper.asLongString(value1));
    rawHeaders.put(key2, LongStringHelper.asLongString(value2.getBytes(StandardCharsets.UTF_8)));

    Map<String, Object> serializedHeaders = RabbitMqMessage.serializableHeaders(rawHeaders);

    assertEquals(value1, serializedHeaders.get(key1));
    assertEquals(value2, serializedHeaders.get(key2));
  }

  @Test
  public void testSerializableHeadersWithListValue() {
    Map<String, Object> rawHeaders = new HashMap<>();
    List<String> expectedSerializedList = Lists.newArrayList("value1", "value2");
    List<LongString> rawList =
        expectedSerializedList.stream()
            .map(LongStringHelper::asLongString)
            .collect(Collectors.toList());
    String key1 = "key1";
    rawHeaders.put(key1, rawList);

    Map<String, Object> serializedHeaders = RabbitMqMessage.serializableHeaders(rawHeaders);

    assertEquals(expectedSerializedList, serializedHeaders.get(key1));
  }

  @Test
  public void testSerializableHeadersWithNestedList() {
    Map<String, Object> serializedHeaders =
        RabbitMqMessage.serializableHeaders(
            ImmutableMap.of(
                "listKey",
                Collections.singletonList(
                    Collections.singletonList(
                        LongStringHelper.asLongString("nestedLongStringVal")))));

    Map<String, Object> expected =
        ImmutableMap.of(
            "listKey", Collections.singletonList(Collections.singletonList("nestedLongStringVal")));
    assertEquals(expected, serializedHeaders);
  }

  @Test
  public void testSerializableHeadersWithLongStringAndNestedList() {
    Map<String, Object> serializedHeaders =
        RabbitMqMessage.serializableHeaders(
            ImmutableMap.of(
                "longStringKey", LongStringHelper.asLongString("longStringVal"),
                "listKey",
                    Collections.singletonList(
                        Collections.singletonList(
                            LongStringHelper.asLongString("nestedLongStringVal")))));

    Map<String, Object> expected =
        ImmutableMap.of(
            "longStringKey",
            "longStringVal",
            "listKey",
            Collections.singletonList(Collections.singletonList("nestedLongStringVal")));
    assertEquals(expected, serializedHeaders);
  }

  @Test
  public void testSerializableHeadersWithNestedMap() {
    Map<String, Object> serializedHeaders =
        RabbitMqMessage.serializableHeaders(
            ImmutableMap.of(
                "mapKey",
                ImmutableMap.of(
                    "nestedLongStringKey", LongStringHelper.asLongString("nestedLongStringVal"))));

    Map<?, ?> expected =
        ImmutableMap.of("mapKey", ImmutableMap.of("nestedLongStringKey", "nestedLongStringVal"));
    assertEquals(expected, serializedHeaders);
  }

  @Test
  public void testSerializableHeadersWithLongStringAndNestedMap() {
    Map<String, Object> serializedHeaders =
        RabbitMqMessage.serializableHeaders(
            ImmutableMap.of(
                "longStringKey", LongStringHelper.asLongString("longStringVal"),
                "mapKey",
                    ImmutableMap.of(
                        "nestedLongStringKey",
                        LongStringHelper.asLongString("nestedLongStringVal"))));

    Map<?, ?> expected =
        ImmutableMap.of(
            "longStringKey",
            "longStringVal",
            "mapKey",
            ImmutableMap.of("nestedLongStringKey", "nestedLongStringVal"));
    assertEquals(expected, serializedHeaders);
  }

  @Test
  public void testSerializableHeader() {
    Integer serializableVal = 1;
    Map<String, Object> headerMap = ImmutableMap.of("serializableKey", serializableVal);
    Map<String, Object> serializedHeaders = RabbitMqMessage.serializableHeaders(headerMap);

    assertEquals(headerMap, serializedHeaders);
  }
}
