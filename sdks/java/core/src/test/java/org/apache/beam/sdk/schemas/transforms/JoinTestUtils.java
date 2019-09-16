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
package org.apache.beam.sdk.schemas.transforms;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

class JoinTestUtils {
  static List<Row> innerJoin(
      List<Row> inputs1, List<Row> inputs2, String[] keys1, String[] keys2, Schema expectedSchema) {
    List<Row> joined = Lists.newArrayList();
    for (Row row1 : inputs1) {
      for (Row row2 : inputs2) {
        List key1 = Arrays.stream(keys1).map(row1::getValue).collect(Collectors.toList());
        List key2 = Arrays.stream(keys2).map(row2::getValue).collect(Collectors.toList());
        if (key1.equals(key2)) {
          joined.add(Row.withSchema(expectedSchema).addValues(row1, row2).build());
        }
      }
    }
    return joined;
  }

  static List<Row> innerJoin(
      List<Row> inputs1,
      List<Row> inputs2,
      List<Row> inputs3,
      String[] keys1,
      String[] keys2,
      String[] keys3,
      Schema expectedSchema) {
    List<Row> joined = Lists.newArrayList();
    for (Row row1 : inputs1) {
      for (Row row2 : inputs2) {
        for (Row row3 : inputs3) {
          List key1 = Arrays.stream(keys1).map(row1::getValue).collect(Collectors.toList());
          List key2 = Arrays.stream(keys2).map(row2::getValue).collect(Collectors.toList());
          List key3 = Arrays.stream(keys3).map(row3::getValue).collect(Collectors.toList());
          if (key1.equals(key2) && key2.equals(key3)) {
            joined.add(Row.withSchema(expectedSchema).addValues(row1, row2, row3).build());
          }
        }
      }
    }
    return joined;
  }
}
