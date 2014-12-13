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

import static com.google.api.client.util.Base64.encodeBase64URLSafeString;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Base64Utils}. */
@RunWith(JUnit4.class)
public class Base64UtilsTest {
  void testLength(int length) {
    byte[] b = new byte[length];
    // Make sure that the estimated length is an upper bound.
    assertThat(
        Base64Utils.getBase64Length(length),
        greaterThanOrEqualTo(encodeBase64URLSafeString(b).length()));
    // Make sure that it's a tight upper bound (no more than 4 characters off).
    assertThat(
        Base64Utils.getBase64Length(length),
        lessThan(4 + encodeBase64URLSafeString(b).length()));
  }

  @Test
  public void getBase64Length() {
    for (int i = 0; i < 100; ++i) {
      testLength(i);
    }
    for (int i = 1000; i < 1100; ++i) {
      testLength(i);
    }
  }
}
