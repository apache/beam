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
import static org.junit.Assert.assertThat;

import com.google.api.services.healthcare.v1beta1.model.ParsedData;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.TypeDescriptor;
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

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    HL7v2Message testValues = createTestValues();
    CoderProperties.coderDecodeEncodeEqual(TEST_CODER, testValues);
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(TYPE_DESCRIPTOR));
  }
}
