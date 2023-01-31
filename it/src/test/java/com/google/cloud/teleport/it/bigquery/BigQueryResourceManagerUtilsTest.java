/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.bigquery;

import static com.google.cloud.teleport.it.bigquery.BigQueryResourceManagerUtils.checkValidTableId;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import org.junit.Test;

/** Unit tests for {@link com.google.cloud.teleport.it.bigquery.BigQueryResourceManagerUtils}. */
public class BigQueryResourceManagerUtilsTest {

  @Test
  public void testCheckValidTableIdWhenIdIsTooShort() {
    assertThrows(IllegalArgumentException.class, () -> checkValidTableId(""));
  }

  @Test
  public void testCheckValidTableIdWhenIdIsTooLong() {
    char[] chars = new char[1025];
    Arrays.fill(chars, 'a');
    String s = new String(chars);
    assertThrows(IllegalArgumentException.class, () -> checkValidTableId(s));
  }

  @Test
  public void testCheckValidTableIdWhenIdContainsIllegalCharacter() {
    assertThrows(IllegalArgumentException.class, () -> checkValidTableId("table-id%"));
    assertThrows(IllegalArgumentException.class, () -> checkValidTableId("ta#ble-id"));
  }

  @Test
  public void testCheckValidTableIdShouldWorkWhenGivenCorrectId() {
    char[] chars = new char[1024];
    Arrays.fill(chars, 'a');
    String s = new String(chars);

    checkValidTableId(s);
    checkValidTableId("a");
    checkValidTableId("this-is_a_valid-id-1");
  }
}
