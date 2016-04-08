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

package com.google.cloud.dataflow.examples;

import com.google.common.base.Splitter;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;


/**
 * Base class for all end-to-end tests for Apache Beam.
 */
public abstract class E2ETest {

  protected Map<String, String> testOptionsMap;

  /**
   * Parses the test options, populates testOptionsMap with them, and validates that required test
   * options are present.
   *
   * @param required A list of test options that are required for the test.
   * @throws IllegalArgumentException if the test is missing testOptions entirely or any required
   *     test options.
   */
  protected void parseTestOptions(String ... required) throws IllegalArgumentException {
    String testOptions = System.getProperty("testOptions");

    if ((testOptions == null) || testOptions.isEmpty()) {
      throw new IllegalArgumentException("End-to-end tests must run with -DtestOptions");
    }

    System.out.println("testOptions = " + testOptions);
    testOptionsMap = Splitter.on(",").withKeyValueSeparator("=").split(testOptions);
    Set<String> missingOptions = new HashSet();

    for (String testOptionKey : required) {
      String testOptionValue = testOptionsMap.get(testOptionKey);
      if ((testOptionValue == null) || testOptionValue.isEmpty()) {
        missingOptions.add(testOptionKey);
      }
    }

    if (!missingOptions.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (String missingOption : missingOptions) {
        sb.append(" ");
        sb.append(missingOption);
      }
      throw new IllegalArgumentException(
          "The following test options must be defined in -DtestOptions:" + sb.toString());
    }
  }

  /**
   * @return String with a unique test identifier based on the current date, time, and a random int.
   */
  protected String generateTestIdentifier() {
    int random = new Random().nextInt(10000);
    DateFormat dateFormat = new SimpleDateFormat("MMddHHmmss");
    Calendar cal = Calendar.getInstance();
    String now = dateFormat.format(cal.getTime());
    return now + random;
  }
}

