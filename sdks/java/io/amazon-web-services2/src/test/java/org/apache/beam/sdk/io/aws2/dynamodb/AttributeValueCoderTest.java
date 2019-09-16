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
package org.apache.beam.sdk.io.aws2.dynamodb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/** Unit test cases for each type of AttributeValue to test encoding and decoding. */
public class AttributeValueCoderTest {

  @Test
  public void shouldPassForStringType() throws IOException {
    AttributeValue expected = AttributeValue.builder().s("test").build();

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForNumberType() throws IOException {
    AttributeValue expected = AttributeValue.builder().n("123").build();

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForBooleanType() throws IOException {
    AttributeValue expected = AttributeValue.builder().bool(false).build();

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForByteArray() throws IOException {
    AttributeValue expected =
        AttributeValue.builder()
            .b(SdkBytes.fromByteArray("hello".getBytes(StandardCharsets.UTF_8)))
            .build();

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForListOfString() throws IOException {
    AttributeValue expected = AttributeValue.builder().ss(ImmutableList.of("foo", "bar")).build();

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForOneListOfNumber() throws IOException {
    AttributeValue expected = AttributeValue.builder().ns(ImmutableList.of("123", "456")).build();

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForOneListOfByteArray() throws IOException {
    AttributeValue expected =
        AttributeValue.builder()
            .bs(
                ImmutableList.of(
                    SdkBytes.fromByteArray("mylistbyte1".getBytes(StandardCharsets.UTF_8)),
                    SdkBytes.fromByteArray(("mylistbyte2".getBytes(StandardCharsets.UTF_8)))))
            .build();

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForListType() throws IOException {
    List<AttributeValue> listAttr = new ArrayList<>();
    listAttr.add(AttributeValue.builder().s("innerMapValue1").build());
    listAttr.add(AttributeValue.builder().n("8976234").build());

    AttributeValue expected = AttributeValue.builder().l(listAttr).build();

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForMapType() throws IOException {

    Map<String, AttributeValue> attrMap = new HashMap<>();
    attrMap.put("innerMapAttr1", AttributeValue.builder().s("innerMapValue1").build());
    attrMap.put(
        "innerMapAttr2",
        AttributeValue.builder()
            .b(SdkBytes.fromByteArray("8976234".getBytes(StandardCharsets.UTF_8)))
            .build());

    AttributeValue expected = AttributeValue.builder().m(attrMap).build();

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void shouldPassForNullType() throws IOException {
    AttributeValue expected = AttributeValue.builder().nul(true).build();

    AttributeValueCoder coder = AttributeValueCoder.of();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());

    AttributeValue actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }
}
