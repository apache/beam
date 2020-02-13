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
package org.apache.beam.sdk.io.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

/** Unit test cases for each type of AttributeValue to test encoding and decoding. */
public class AttributeValueCoderTest {

  @Test
  public void shouldPassForStringType() throws IOException {
    AttributeValue expected = new AttributeValue();
    expected.setS("testing");

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForNumberType() throws IOException {
    AttributeValue expected = new AttributeValue();
    expected.setN("123");

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForBooleanType() throws IOException {
    AttributeValue expected = new AttributeValue();
    expected.setBOOL(false);

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForByteArray() throws IOException {
    AttributeValue expected = new AttributeValue();
    expected.setB(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)));

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForListOfString() throws IOException {
    AttributeValue expected = new AttributeValue();
    expected.setSS(ImmutableList.of("foo", "bar"));

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForOneListOfNumber() throws IOException {
    AttributeValue expected = new AttributeValue();
    expected.setNS(ImmutableList.of("123", "456"));

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForOneListOfByteArray() throws IOException {
    AttributeValue expected = new AttributeValue();
    expected.setBS(
        ImmutableList.of(
            ByteBuffer.wrap("mylistbyte1".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap("mylistbyte2".getBytes(StandardCharsets.UTF_8))));

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForListType() throws IOException {
    AttributeValue expected = new AttributeValue();

    List<AttributeValue> listAttr = new ArrayList<>();
    listAttr.add(new AttributeValue("innerMapValue1"));
    listAttr.add(new AttributeValue().withN("8976234"));

    expected.setL(listAttr);

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForMapType() throws IOException {
    AttributeValue expected = new AttributeValue();

    Map<String, AttributeValue> attrMap = new HashMap<>();
    attrMap.put("innerMapAttr1", new AttributeValue("innerMapValue1"));
    attrMap.put(
        "innerMapAttr2",
        new AttributeValue().withB(ByteBuffer.wrap("8976234".getBytes(StandardCharsets.UTF_8))));

    expected.setM(attrMap);

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForNullType() throws IOException {
    AttributeValue expected = new AttributeValue();
    expected.setNULL(true);

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }
}
