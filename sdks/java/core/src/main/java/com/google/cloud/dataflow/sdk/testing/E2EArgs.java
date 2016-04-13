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
package com.google.cloud.dataflow.sdk.testing;

import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * {@link E2EArgs} is a set of args/utilities for use by Beam End-to-End tests.
 */
public class E2EArgs {
  private Map<String, String> args;
  private static Map<String, String> testOptionsMap;

  /**
   * Constructs a set of args from the provided map.
   */
  public E2EArgs(Map<String, String> args) {
    this.args = args;
  }

  /**
   * Adds a key/value pair to the map of args.
   */
  public void add(String key, String val) {
    args.put(key, val);
  }

  /**
   * Gets the job name argument.
   *
   * @return Name of the job to be run.
   */
  public String getArg(String key) {
    return args.get(key);
  }

  /**
   * Builds a string array of args which can be used by an end-to-end test.
   *
   * @return Test args for an end-to-end test.
   */
  public String[] build() {
    ArrayList<String> optArrayList = new ArrayList<>();
    for (Map.Entry<String, String> entry : args.entrySet()) {
      optArrayList.add("--" + entry.getKey() + "=" + entry.getValue());
    }
    return optArrayList.toArray(new String[optArrayList.size()]);
  }

  /**
   * Parses the test options, populates testOptionsMap with them, and validates that required test
   * options are present.
   *
   * @param required A list of test options that are required for the test.
   * @throws IllegalArgumentException if the test is missing testOptions entirely or any required
   *     test options.
   */
  public static void parseTestOptions(String ... required) throws IllegalArgumentException {
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

  public static String getTestFileLocation() {
    String testFileLocation = testOptionsMap.get("testFileLocation");
    if (!testFileLocation.endsWith("/")) {
      testFileLocation = testFileLocation + "/";
    }
    return testFileLocation;
  }
}

