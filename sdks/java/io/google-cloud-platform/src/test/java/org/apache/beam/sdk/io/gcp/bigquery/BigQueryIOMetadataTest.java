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
package org.apache.beam.sdk.io.gcp.bigquery;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for BigQueryIOMetadataTest. */
@RunWith(JUnit4.class)
public class BigQueryIOMetadataTest {

  @Test
  public void testIsValidCloudLabel() {
    // A dataflow job ID.
    // Lowercase letters, numbers, underscores and hyphens are allowed.
    String testStr = "2020-06-29_15_26_09-12838749047888422749";
    assertTrue(BigQueryIOMetadata.isValidCloudLabel(testStr));

    // At least one character.
    testStr = "0";
    assertTrue(BigQueryIOMetadata.isValidCloudLabel(testStr));

    // Up to 63 characters.
    testStr = "0123456789abcdefghij0123456789abcdefghij0123456789abcdefghij012";
    assertTrue(BigQueryIOMetadata.isValidCloudLabel(testStr));

    // Lowercase letters allowed
    testStr = "abcdefghijklmnopqrstuvwxyz";
    for (char testChar : testStr.toCharArray()) {
      assertTrue(BigQueryIOMetadata.isValidCloudLabel(String.valueOf(testChar)));
    }

    // Empty strings not allowed.
    testStr = "";
    assertFalse(BigQueryIOMetadata.isValidCloudLabel(testStr));

    // 64 or more characters not allowed.
    testStr = "0123456789abcdefghij0123456789abcdefghij0123456789abcdefghij0123";
    assertFalse(BigQueryIOMetadata.isValidCloudLabel(testStr));

    // Uppercase letters not allowed
    testStr = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    for (char testChar : testStr.toCharArray()) {
      assertFalse(BigQueryIOMetadata.isValidCloudLabel(String.valueOf(testChar)));
    }

    // Special characters besides hyphens are not allowed
    testStr = "!@#$%^&*()+=[{]};:\'\"\\|,<.>?/`~";
    for (char testChar : testStr.toCharArray()) {
      assertFalse(BigQueryIOMetadata.isValidCloudLabel(String.valueOf(testChar)));
    }
  }
}
