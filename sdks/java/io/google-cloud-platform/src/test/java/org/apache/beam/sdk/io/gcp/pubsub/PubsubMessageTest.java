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
package org.apache.beam.sdk.io.gcp.pubsub;

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage.PubsubValidator.*;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;

@RunWith(JUnit4.class)
public class PubsubMessageTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  static byte[] asBytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  static String makeStringOfLength(int length) {
    return StringUtils.repeat("a", length);
  }

  @Test
  public void testValidPubSubMessagePayload() {
    byte[] bytes = asBytes(makeStringOfLength(MAX_PUBLISH_BYTE_SIZE));
    PubsubMessage message = new PubsubMessage(bytes, new HashMap<String, String>());
    Assert.assertTrue(Arrays.equals(message.getPayload(), bytes));
  }

  @Test
  public void testValidPubSubMessageAttributes() {
    HashMap<String, String> testMap = new HashMap();
    testMap.put(makeStringOfLength(MAX_ATTRIBUTE_KEY_SIZE),
      makeStringOfLength(MAX_ATTRIBUTE_VALUE_SIZE));
    PubsubMessage message = new PubsubMessage(asBytes(""), testMap);
    Assert.assertEquals(testMap, message.getAttributeMap());

    testMap.clear();
    for (int i = 0; i < MAX_MSG_ATTRIBUTES; i++) {
      testMap.put(String.valueOf(i), "");
    }
    message = new PubsubMessage(asBytes(""), testMap);
    Assert.assertEquals(testMap, message.getAttributeMap());
  }

  @Test
  public void testInvalidMessagePayload() {
    thrown.expect(IllegalStateException.class);
    String invalidPayloadString = makeStringOfLength(MAX_PUBLISH_BYTE_SIZE + 1);
    new PubsubMessage(asBytes(invalidPayloadString), new HashMap<String, String>());
  }

  @Test
  public void testInvalidAttributeCount() {
    thrown.expect(IllegalStateException.class);
    HashMap<String, String> testMap = new HashMap();
    for (int i = 0; i < MAX_MSG_ATTRIBUTES + 1; i++) {
      testMap.put(String.valueOf(i), "");
    }
    new PubsubMessage(asBytes(""), testMap);
  }

  @Test
  public void testInvalidAttributeKeySize() {
    thrown.expect(IllegalStateException.class);
    HashMap<String, String> testMap = new HashMap();
    testMap.put(makeStringOfLength(MAX_ATTRIBUTE_KEY_SIZE + 1), "");
    new PubsubMessage(asBytes(""), testMap);
  }

  @Test
  public void testInvalidAttributeValueSize() {
    thrown.expect(IllegalStateException.class);
    HashMap<String, String> testMap = new HashMap();
    testMap.put("", makeStringOfLength(MAX_ATTRIBUTE_VALUE_SIZE + 1));
    new PubsubMessage(asBytes(""), testMap);
  }
}
