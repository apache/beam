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

import static java.util.Objects.requireNonNull;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.allPrimitiveDataTypes;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.allPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.nullableAllPrimitiveDataTypes;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.nullableAllPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.timeContaining;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.timeContainingToRowFn;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

/** Shared data for use in {@link CsvIO} tests and related classes. */
class CsvIOTestData {
  static final CsvIOTestData DATA = new CsvIOTestData();

  private CsvIOTestData() {}

  final Row allPrimitiveDataTypesRow =
      requireNonNull(
          allPrimitiveDataTypesToRowFn()
              .apply(
                  allPrimitiveDataTypes(
                      false, (byte) 1, BigDecimal.TEN, 1.0, 1.0f, (short) 1.0, 1, 1L, "a")));

  final Row allPrimitiveDataTypesRowWithPadding =
      requireNonNull(
          allPrimitiveDataTypesToRowFn()
              .apply(
                  allPrimitiveDataTypes(
                      false,
                      (byte) 1,
                      BigDecimal.TEN,
                      1.0,
                      1.0f,
                      (short) 1.0,
                      1,
                      1L,
                      "       a           ")));

  final List<Row> allPrimitiveDataTypeRows =
      Stream.of(
              allPrimitiveDataTypes(
                  false, (byte) 1, BigDecimal.TEN, 1.0, 1.0f, (short) 1.0, 1, 1L, "a"),
              allPrimitiveDataTypes(
                  false,
                  (byte) 2,
                  BigDecimal.TEN.add(BigDecimal.TEN),
                  2.0,
                  2.0f,
                  (short) 2.0,
                  2,
                  2L,
                  "b"),
              allPrimitiveDataTypes(
                  false,
                  (byte) 3,
                  BigDecimal.TEN.add(BigDecimal.TEN).add(BigDecimal.TEN),
                  3.0,
                  3.0f,
                  (short) 3.0,
                  3,
                  3L,
                  "c"))
          .map(allPrimitiveDataTypesToRowFn()::apply)
          .collect(Collectors.toList());

  final Row nullableTypesRowAllNull =
      requireNonNull(
          nullableAllPrimitiveDataTypesToRowFn()
              .apply(nullableAllPrimitiveDataTypes(null, null, null, null, null, null)));

  final Row nullableTypesRowSomeNull =
      requireNonNull(
          nullableAllPrimitiveDataTypesToRowFn()
              .apply(nullableAllPrimitiveDataTypes(true, null, null, 1, null, "a")));

  final Row timeContainingRow =
      timeContainingToRowFn()
          .apply(
              timeContaining(
                  Instant.ofEpochMilli(1L), Collections.singletonList(Instant.ofEpochMilli(1L))));
}
