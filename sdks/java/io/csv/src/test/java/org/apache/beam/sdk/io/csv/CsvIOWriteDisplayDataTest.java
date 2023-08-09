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
package org.apache.beam.sdk.io.csv;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CsvIO.Write#populateDisplayData} using various {@link
 * org.apache.commons.csv.CSVFormat} settings.
 */
@RunWith(JUnit4.class)
public class CsvIOWriteDisplayDataTest {
  private static final String CRLF = "\r\n";

  @Test
  public void csvFormatNotPredefined() {
    CSVFormat csvFormat = CSVFormat.newFormat(',');
    assertDisplayDataEquals(
        ImmutableMap.of(
            "trim", false,
            "delimiter", ",",
            "allowDuplicateHeaderNames", true,
            "trailingDelimiter", false),
        DisplayData.from(CsvIO.write("somewhere", csvFormat)),
        "CSVFormat from newFormat");
  }

  @Test
  public void defaultPredefinedCsvFormat() {
    CSVFormat csvFormat = CSVFormat.DEFAULT;
    assertDisplayDataEquals(
        ImmutableMap.<String, Object>builder()
            .put("recordSeparator", CRLF)
            .put("quoteCharacter", "\"")
            .put("allowDuplicateHeaderNames", true)
            .put("delimiter", ",")
            .put("trim", false)
            .put("trailingDelimiter", false)
            .build(),
        DisplayData.from(CsvIO.write("somewhere", csvFormat)),
        CSVFormat.Predefined.Default.name());
  }

  private static void assertDisplayDataEquals(
      ImmutableMap<String, Object> expected, DisplayData actual, String message) {
    Map<String, Object> actualMap = new HashMap<>();
    for (DisplayData.Item item : actual.items()) {
      actualMap.put(item.getKey(), item.getValue());
    }
    assertEquals(message, expected, actualMap);
  }
}
