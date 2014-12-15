/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.util;

import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for SerializableUtils.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class SerializableUtilsTest {
  static class TestClass implements Serializable {
    final String stringValue;
    final int intValue;

    public TestClass(String stringValue, int intValue) {
      this.stringValue = stringValue;
      this.intValue = intValue;
    }
  }

  @Test
  public void testTranscode() {
    String stringValue = "hi bob";
    int intValue = 42;

    TestClass testObject = new TestClass(stringValue, intValue);

    Object copy =
        SerializableUtils.deserializeFromByteArray(
            SerializableUtils.serializeToByteArray(testObject),
            "a TestObject");

    Assert.assertThat(copy, new IsInstanceOf(TestClass.class));
    TestClass testCopy = (TestClass) copy;

    Assert.assertEquals(stringValue, testCopy.stringValue);
    Assert.assertEquals(intValue, testCopy.intValue);
  }

  @Test
  public void testDeserializationError() {
    try {
      SerializableUtils.deserializeFromByteArray(
          "this isn't legal".getBytes(),
          "a bogus string");
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        CoreMatchers.containsString(
                            "unable to deserialize a bogus string"));
    }
  }
}
