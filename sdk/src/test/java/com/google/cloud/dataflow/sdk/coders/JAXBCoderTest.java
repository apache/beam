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

import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.xml.bind.annotation.XmlRootElement;

/** Unit tests for {@link JAXBCoder}. */
@RunWith(JUnit4.class)
public class JAXBCoderTest {

  @XmlRootElement
  static class TestType {
    private String testString = null;
    private int testInt;

    public TestType() {}

    public TestType(String testString, int testInt) {
      this.testString = testString;
      this.testInt = testInt;
    }

    public String getTestString() {
      return testString;
    }

    public void setTestString(String testString) {
      this.testString = testString;
    }

    public int getTestInt() {
      return testInt;
    }

    public void setTestInt(int testInt) {
      this.testInt = testInt;
    }

    @Override
    public int hashCode() {
      int hashCode = 1;
      hashCode = 31 * hashCode + (testString == null ? 0 : testString.hashCode());
      hashCode = 31 * hashCode + testInt;
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof TestType)) {
        return false;
      }

      TestType other = (TestType) obj;
      return (testString == null || testString.equals(other.testString))
          && (testInt == other.testInt);
    }
  }

  @Test
  public void testEncodeDecode() throws Exception {
    JAXBCoder<TestType> coder = JAXBCoder.of(TestType.class);

    byte[] encoded = CoderUtils.encodeToByteArray(coder, new TestType("abc", 9999));
    Assert.assertEquals(new TestType("abc", 9999), CoderUtils.decodeFromByteArray(coder, encoded));
  }

  @Test
  public void testEncodable() throws Exception {
    CoderProperties.coderSerializable(JAXBCoder.of(TestType.class));
  }

  @Test
  public void testEncodingId() throws Exception {
    Coder<TestType> coder = JAXBCoder.of(TestType.class);
    CoderProperties.coderHasEncodingId(
        coder, TestType.class.getName());
  }
}
