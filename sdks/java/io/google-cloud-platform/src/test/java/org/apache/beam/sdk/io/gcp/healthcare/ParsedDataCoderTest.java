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
import com.google.api.services.healthcare.v1beta1.model.Segment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link ParsedDataCoder}. */
@RunWith(JUnit4.class)
public class ParsedDataCoderTest {

  static class ParsedDataBuilder {
    private List<Segment> segments;

    public ParsedDataBuilder() {
      segments = new ArrayList<>();
    }

    public ParsedDataCoderTest.ParsedDataBuilder addSegment(Segment value) {
      segments.add(value);
      return this;
    }

    public ParsedData build() {
      return new ParsedData().setSegments(this.segments);
    }
  }

  private static final Coder<ParsedData> TEST_CODER = ParsedDataCoder.of();

  private static Integer generateRandomNumber(int low, int high) {
    return Integer.valueOf(new Random().nextInt(high - low) + low);
  }

  private static String generateRandomString(int length) {
    // chose a Character random from this String
    String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvxyz";

    // create StringBuffer size of AlphaNumericString
    StringBuilder sb = new StringBuilder(length);

    for (int i = 0; i < length; i++) {

      // generate a random number between
      // 0 to AlphaNumericString variable length
      int index = (int) (alphaNumericString.length() * Math.random());
      // add Character one by one in end of sb
      sb.append(alphaNumericString.charAt(index));
    }

    return sb.toString();
  }

  private static Segment createSegment() {
    Map<String, String> fieldMap = new HashMap<>();
    for (int x = 0; x < generateRandomNumber(0, 5).intValue(); x++) {
      Integer fieldNumber = Integer.valueOf(x);
      fieldMap.put(fieldNumber.toString(), generateRandomString(25));
    }
    return new Segment().setFields(fieldMap).setSegmentId(generateRandomString(3));
  }

  static ParsedData createTestValues() {
    ParsedDataBuilder parsedDataBuilder = new ParsedDataBuilder();
    for (int x = 0; x < generateRandomNumber(0, 5).intValue(); x++) {
      parsedDataBuilder.addSegment(createSegment());
    }
    return parsedDataBuilder.build();
  }

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    ParsedData testValues = createTestValues();
    CoderProperties.coderDecodeEncodeEqual(TEST_CODER, testValues);
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(TypeDescriptor.of(ParsedData.class)));
  }
}
