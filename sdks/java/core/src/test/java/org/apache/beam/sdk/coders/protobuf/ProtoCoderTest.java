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
package org.apache.beam.sdk.coders.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.cloud.dataflow.sdk.coders.Proto2CoderTestMessages;
import com.google.cloud.dataflow.sdk.coders.Proto2CoderTestMessages.MessageA;
import com.google.cloud.dataflow.sdk.coders.Proto2CoderTestMessages.MessageB;
import com.google.cloud.dataflow.sdk.coders.Proto2CoderTestMessages.MessageC;
import com.google.cloud.dataflow.sdk.coders.Proto2CoderTestMessages.MessageWithMap;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ProtoCoder}.
 */
@RunWith(JUnit4.class)
public class ProtoCoderTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testFactoryMethodAgreement() throws Exception {
    assertEquals(ProtoCoder.of(new TypeDescriptor<MessageA>() {}), ProtoCoder.of(MessageA.class));

    assertEquals(
        ProtoCoder.of(new TypeDescriptor<MessageA>() {}),
        ProtoCoder.coderProvider().getCoder(new TypeDescriptor<MessageA>() {}));
  }

  @Test
  public void testProviderCannotProvideCoder() throws Exception {
    thrown.expect(CannotProvideCoderException.class);
    thrown.expectMessage("java.lang.Integer is not a subclass of com.google.protobuf.Message");

    ProtoCoder.coderProvider().getCoder(new TypeDescriptor<Integer>() {});
  }

  @Test
  public void testCoderEncodeDecodeEqual() throws Exception {
    MessageA value =
        MessageA.newBuilder()
            .setField1("hello")
            .addField2(MessageB.newBuilder().setField1(true).build())
            .addField2(MessageB.newBuilder().setField1(false).build())
            .build();
    CoderProperties.coderDecodeEncodeEqual(ProtoCoder.of(MessageA.class), value);
  }

  @Test
  public void testCoderEncodeDecodeEqualNestedContext() throws Exception {
    MessageA value1 =
        MessageA.newBuilder()
            .setField1("hello")
            .addField2(MessageB.newBuilder().setField1(true).build())
            .addField2(MessageB.newBuilder().setField1(false).build())
            .build();
    MessageA value2 =
        MessageA.newBuilder()
            .setField1("world")
            .addField2(MessageB.newBuilder().setField1(false).build())
            .addField2(MessageB.newBuilder().setField1(true).build())
            .build();
    CoderProperties.coderDecodeEncodeEqual(
        ListCoder.of(ProtoCoder.of(MessageA.class)), ImmutableList.of(value1, value2));
  }

  @Test
  public void testCoderEncodeDecodeExtensionsEqual() throws Exception {
    MessageC value =
        MessageC.newBuilder()
            .setExtension(
                Proto2CoderTestMessages.field1,
                MessageA.newBuilder()
                    .setField1("hello")
                    .addField2(MessageB.newBuilder().setField1(true).build())
                    .build())
            .setExtension(
                Proto2CoderTestMessages.field2, MessageB.newBuilder().setField1(false).build())
            .build();
    CoderProperties.coderDecodeEncodeEqual(
        ProtoCoder.of(MessageC.class).withExtensionsFrom(Proto2CoderTestMessages.class), value);
  }

  @Test
  public void testCoderSerialization() throws Exception {
    ProtoCoder<MessageA> coder = ProtoCoder.of(MessageA.class);
    CoderProperties.coderSerializable(coder);
  }

  @Test
  public void testCoderExtensionsSerialization() throws Exception {
    ProtoCoder<MessageC> coder =
        ProtoCoder.of(MessageC.class).withExtensionsFrom(Proto2CoderTestMessages.class);
    CoderProperties.coderSerializable(coder);
  }

  @Test
  public void testEncodingId() throws Exception {
    Coder<MessageA> coderA = ProtoCoder.of(MessageA.class);
    CoderProperties.coderHasEncodingId(coderA, MessageA.class.getName() + "[]");

    ProtoCoder<MessageC> coder =
        ProtoCoder.of(MessageC.class).withExtensionsFrom(Proto2CoderTestMessages.class);
    CoderProperties.coderHasEncodingId(
        coder,
        String.format("%s[%s]", MessageC.class.getName(), Proto2CoderTestMessages.class.getName()));
  }

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null MessageA");

    CoderUtils.encodeToBase64(ProtoCoder.of(MessageA.class), null);
  }

  @Test
  public void testDeterministicCoder() throws NonDeterministicException {
    Coder<MessageA> coder = ProtoCoder.of(MessageA.class);
    coder.verifyDeterministic();
  }

  @Test
  public void testNonDeterministicCoder() throws NonDeterministicException {
    thrown.expect(NonDeterministicException.class);
    thrown.expectMessage(MessageWithMap.class.getName() + " transitively includes Map field");

    Coder<MessageWithMap> coder = ProtoCoder.of(MessageWithMap.class);
    coder.verifyDeterministic();
  }

  @Test
  public void testNonDeterministicProperty() throws CoderException {
    MessageWithMap.Builder msg1B = MessageWithMap.newBuilder();
    MessageWithMap.Builder msg2B = MessageWithMap.newBuilder();

    // Built in reverse order but with equal contents.
    for (int i = 0; i < 10; ++i) {
      msg1B.getMutableField1().put("key" + i, MessageA.getDefaultInstance());
      msg2B.getMutableField1().put("key" + (9 - i), MessageA.getDefaultInstance());
    }

    // Assert the messages are equal.
    MessageWithMap msg1 = msg1B.build();
    MessageWithMap msg2 = msg2B.build();
    assertEquals(msg2, msg1);

    // Assert the encoded messages are not equal.
    Coder<MessageWithMap> coder = ProtoCoder.of(MessageWithMap.class);
    assertNotEquals(CoderUtils.encodeToBase64(coder, msg2), CoderUtils.encodeToBase64(coder, msg1));
  }
}
