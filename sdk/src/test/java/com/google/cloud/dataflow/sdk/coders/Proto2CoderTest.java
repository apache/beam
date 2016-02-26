/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.coders.Proto2CoderTestMessages.MessageA;
import com.google.cloud.dataflow.sdk.coders.Proto2CoderTestMessages.MessageB;
import com.google.cloud.dataflow.sdk.coders.Proto2CoderTestMessages.MessageC;
import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.collect.ImmutableList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for Proto2Coder.
 */
@SuppressWarnings("deprecation") // test of a deprecated coder.
@RunWith(JUnit4.class)
public class Proto2CoderTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testFactoryMethodAgreement() throws Exception {
    assertEquals(
        Proto2Coder.of(new TypeDescriptor<MessageA>() {}),
        Proto2Coder.of(MessageA.class));

    assertEquals(
        Proto2Coder.of(new TypeDescriptor<MessageA>() {}),
        Proto2Coder.coderProvider().getCoder(new TypeDescriptor<MessageA>() {}));
  }

  @Test
  public void testProviderCannotProvideCoder() throws Exception {
    thrown.expect(CannotProvideCoderException.class);
    Proto2Coder.coderProvider().getCoder(new TypeDescriptor<Integer>() {});
  }

  @Test
  public void testCoderEncodeDecodeEqual() throws Exception {
    MessageA value = MessageA.newBuilder()
        .setField1("hello")
        .addField2(MessageB.newBuilder()
            .setField1(true).build())
        .addField2(MessageB.newBuilder()
            .setField1(false).build())
        .build();
    CoderProperties.coderDecodeEncodeEqual(Proto2Coder.of(MessageA.class), value);
  }

  @Test
  public void testCoderEncodeDecodeEqualNestedContext() throws Exception {
    MessageA value1 = MessageA.newBuilder()
        .setField1("hello")
        .addField2(MessageB.newBuilder()
            .setField1(true).build())
        .addField2(MessageB.newBuilder()
            .setField1(false).build())
        .build();
    MessageA value2 = MessageA.newBuilder()
        .setField1("world")
        .addField2(MessageB.newBuilder()
            .setField1(false).build())
        .addField2(MessageB.newBuilder()
            .setField1(true).build())
        .build();
    CoderProperties.coderDecodeEncodeEqual(
        ListCoder.of(Proto2Coder.of(MessageA.class)),
        ImmutableList.of(value1, value2));
  }

  @Test
  public void testCoderEncodeDecodeExtensionsEqual() throws Exception {
    MessageC value = MessageC.newBuilder()
        .setExtension(Proto2CoderTestMessages.field1,
            MessageA.newBuilder()
            .setField1("hello")
            .addField2(MessageB.newBuilder()
                .setField1(true)
                .build())
            .build())
        .setExtension(Proto2CoderTestMessages.field2,
            MessageB.newBuilder()
            .setField1(false)
            .build())
        .build();
    CoderProperties.coderDecodeEncodeEqual(
        Proto2Coder.of(MessageC.class).withExtensionsFrom(Proto2CoderTestMessages.class),
        value);
  }

  @Test
  public void testCoderSerialization() throws Exception {
    Proto2Coder<MessageA> coder = Proto2Coder.of(MessageA.class);
    CoderProperties.coderSerializable(coder);
  }

  @Test
  public void testCoderExtensionsSerialization() throws Exception {
    Proto2Coder<MessageC> coder = Proto2Coder.of(MessageC.class)
        .withExtensionsFrom(Proto2CoderTestMessages.class);
    CoderProperties.coderSerializable(coder);
  }

  @Test
  public void testEncodingId() throws Exception {
    Coder<MessageA> coderA = Proto2Coder.of(MessageA.class);
    CoderProperties.coderHasEncodingId(coderA, MessageA.class.getName());

    Proto2Coder<MessageC> coder = Proto2Coder.of(MessageC.class)
        .withExtensionsFrom(Proto2CoderTestMessages.class);
    CoderProperties.coderHasEncodingId(coder, MessageC.class.getName());
  }

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null MessageA");

    CoderUtils.encodeToBase64(Proto2Coder.of(MessageA.class), null);
  }
}
