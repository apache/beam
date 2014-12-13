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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests for StringUtils.
 */
@RunWith(JUnit4.class)
public class StringUtilsTest {
  @Test
  public void testTranscodeEmptyByteArray() {
    byte[] bytes = { };
    String string = "";
    assertEquals(string, StringUtils.byteArrayToJsonString(bytes));
    assertArrayEquals(bytes, StringUtils.jsonStringToByteArray(string));
  }

  @Test
  public void testTranscodeMixedByteArray() {
    byte[] bytes = {
      0, 5, 12, 16, 31, 32, 65, 66, 126, 127, (byte) 128, (byte) 255, 67, 0 };
    String string = "%00%05%0c%10%1f AB~%7f%80%ffC%00";
    assertEquals(string, StringUtils.byteArrayToJsonString(bytes));
    assertArrayEquals(bytes, StringUtils.jsonStringToByteArray(string));
  }

  /**
   * Inner class for simple name test.
   */
  private class EmbeddedDoFn {
    // Returns an anonymous inner class.
    private EmbeddedDoFn getEmbedded() {
      return new EmbeddedDoFn(){};
    }
  }

  @Test
  public void testSimpleName() {
    assertEquals("Embedded",
        StringUtils.approximateSimpleName(EmbeddedDoFn.class));
  }

  @Test
  public void testAnonSimpleName() {
    EmbeddedDoFn anon = new EmbeddedDoFn(){};

    Pattern p = Pattern.compile("StringUtilsTest\\$[0-9]+");
    Matcher m = p.matcher(StringUtils.approximateSimpleName(anon.getClass()));
    assertThat(m.matches(), is(true));
  }

  @Test
  public void testNestedSimpleName() {
    EmbeddedDoFn fn = new EmbeddedDoFn();
    EmbeddedDoFn anon = fn.getEmbedded();

    // Expect to find "Embedded$1"
    Pattern p = Pattern.compile("Embedded\\$[0-9]+");
    Matcher m = p.matcher(StringUtils.approximateSimpleName(anon.getClass()));
    assertThat(m.matches(), is(true));
  }
}
