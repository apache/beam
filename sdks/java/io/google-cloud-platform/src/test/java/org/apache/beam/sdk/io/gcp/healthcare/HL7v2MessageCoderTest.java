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
package org.apache.beam.sdk.io.gcp.healthcare;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.api.services.healthcare.v1beta1.model.ParsedData;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.UnownedInputStream;
import org.apache.beam.sdk.util.UnownedOutputStream;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CountingInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link HL7v2MessageCoder}. */
@RunWith(JUnit4.class)
public class HL7v2MessageCoderTest {

  private static final HL7v2MessageCoder TEST_CODER = HL7v2MessageCoder.of();
  private static final TypeDescriptor<HL7v2Message> TYPE_DESCRIPTOR =
      new TypeDescriptor<HL7v2Message>() {};

  private HL7v2Message createTestValues() {
    ParsedData parsedData = ParsedDataCoderTest.createTestValues();
    System.out.println("HL7v2 Parsed Data value: " + parsedData.toString());
    HL7v2Message.HL7v2MessageBuilder mb =
        new HL7v2Message.HL7v2MessageBuilder(
                "Mh2RWJZpqdDEAxFr4M0sQZv7lbQuA-Uxe-h9uLjR4j8=",
                "ADT",
                "2002-08-22T17:41:06Z",
                "2020-07-16T15:54:50.667552Z",
                "Test Data",
                "RAL")
            .setParsedData(parsedData);
    return new HL7v2Message(mb);
  }

  @VisibleForTesting
  static byte[] encode(HL7v2MessageCoder coder, Coder.Context context, HL7v2Message value)
      throws CoderException, IOException {
    @SuppressWarnings("unchecked")
    HL7v2MessageCoder deserializedCoder = SerializableUtils.clone(coder);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    deserializedCoder.encode(value, new UnownedOutputStream(os), context);
    return os.toByteArray();
  }

  @VisibleForTesting
  static HL7v2Message decode(HL7v2MessageCoder coder, Coder.Context context, byte[] bytes)
      throws CoderException, IOException {
    @SuppressWarnings("unchecked")
    HL7v2MessageCoder deserializedCoder = SerializableUtils.clone(coder);

    byte[] buffer;
    if (Objects.equals(context, Coder.Context.NESTED)) {
      buffer = new byte[bytes.length + 1];
      System.arraycopy(bytes, 0, buffer, 0, bytes.length);
      buffer[bytes.length] = 1;
    } else {
      buffer = bytes;
    }

    CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(buffer));
    HL7v2Message value = deserializedCoder.decode(new UnownedInputStream(cis), context);
    assertThat(
        "consumed bytes equal to encoded bytes", cis.getCount(), equalTo((long) bytes.length));
    return value;
  }

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    HL7v2Message testValue = createTestValues();
    byte[] encodedValue = encode(TEST_CODER, Coder.Context.NESTED, testValue);
    HL7v2Message decodedValue = decode(TEST_CODER, Coder.Context.NESTED, encodedValue);
    System.out.println("testValue: " + testValue.toString());
    System.out.println("testValue Hash Code: " + testValue.hashCode());
    System.out.println("decodedValue: " + decodedValue.toString());
    System.out.println("decodedValue Hash Code: " + decodedValue.hashCode());
    if (decodedValue.equals(testValue)) {
      System.out.println("decoded value and test value are equal...");
    } else {
      System.out.println("decodedValue and testValue are not equal");
      System.out.println("name is equal: " + decodedValue.getName().equals(testValue.getName()));
      System.out.println(
          "createTime is equal: " + decodedValue.getCreateTime().equals(testValue.getCreateTime()));
      System.out.println("data is equal: " + decodedValue.getData().equals(testValue.getData()));
      if (decodedValue.getLabels() != null) {
        System.out.println(
            "Labels are equal: " + decodedValue.getLabels().equals(testValue.getLabels()));
      } else {
        System.out.println("Labels are null");
      }
      System.out.println(
          "messageType is equal: "
              + decodedValue.getMessageType().equals(testValue.getMessageType()));
      if (decodedValue.getSchematizedData() != null) {
        System.out.println(
            "schematizedData is equal: "
                + decodedValue.getSchematizedData().equals(testValue.getSchematizedData()));
      } else {
        System.out.println("schematizedData is null");
      }
      System.out.println(
          "sendFacility is equal: "
              + decodedValue.getSendFacility().equals(testValue.getSendFacility()));
      System.out.println(
          "sendTime is equal: " + decodedValue.getSendTime().equals(testValue.getSendTime()));
      if (decodedValue.getParsedData() != null) {
        System.out.println(
            "is parsedData equal? : "
                + decodedValue.getParsedData().equals(testValue.getParsedData()));
      } else {
        System.out.println("parsedData is null");
      }
    }
    assertEquals(decodedValue, testValue);
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(TYPE_DESCRIPTOR));
  }
}
