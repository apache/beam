/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat;

import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.Text;

/**
 * Test Utils used in {@link EmployeeInputFormat} and {@link ReuseObjectsEmployeeInputFormat} for
 * computing splits.
 */
public class TestEmployeeDataSet {
  public static final long NUMBER_OF_RECORDS_IN_EACH_SPLIT = 5L;
  public static final long NUMBER_OF_SPLITS = 3L;
  private static final List<KV<String, String>> data = new ArrayList<>();

  /**
   * Returns List of employee details. Employee details are available in the form of {@link KV} in
   * which, key indicates employee id and value indicates employee details such as name and address
   * separated by '_'. This is data input to {@link EmployeeInputFormat} and {@link
   * ReuseObjectsEmployeeInputFormat}.
   */
  public static List<KV<String, String>> populateEmployeeData() {
    if (!data.isEmpty()) {
      return data;
    }
    data.add(KV.of("0", "Alex_US"));
    data.add(KV.of("1", "John_UK"));
    data.add(KV.of("2", "Tom_UK"));
    data.add(KV.of("3", "Nick_UAE"));
    data.add(KV.of("4", "Smith_IND"));
    data.add(KV.of("5", "Taylor_US"));
    data.add(KV.of("6", "Gray_UK"));
    data.add(KV.of("7", "James_UAE"));
    data.add(KV.of("8", "Jordan_IND"));
    data.add(KV.of("9", "Leena_UK"));
    data.add(KV.of("10", "Zara_UAE"));
    data.add(KV.of("11", "Talia_IND"));
    data.add(KV.of("12", "Rose_UK"));
    data.add(KV.of("13", "Kelvin_UAE"));
    data.add(KV.of("14", "Goerge_IND"));
    return data;
  }

  /**
   * This is a helper function used in unit tests for validating data against data read using {@link
   * EmployeeInputFormat} and {@link ReuseObjectsEmployeeInputFormat}.
   */
  public static List<KV<Text, Employee>> getEmployeeData() {
    return (data.isEmpty() ? populateEmployeeData() : data)
        .stream()
        .map(
            input -> {
              List<String> empData = Splitter.on('_').splitToList(input.getValue());
              return KV.of(new Text(input.getKey()), new Employee(empData.get(0), empData.get(1)));
            })
        .collect(Collectors.toList());
  }
}
