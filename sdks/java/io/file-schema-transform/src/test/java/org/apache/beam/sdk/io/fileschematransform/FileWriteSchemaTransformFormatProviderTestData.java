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
package org.apache.beam.sdk.io.fileschematransform;

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.arrayPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.arrayPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.byteSequenceType;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.byteSequenceTypeToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.byteType;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.byteTypeToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.doublyNestedDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.doublyNestedDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.singlyNestedDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.singlyNestedDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.timeContaining;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.timeContainingToRowFn;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.AllPrimitiveDataTypes;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ArrayPrimitiveDataTypes;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ByteSequenceType;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ByteType;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.DoublyNestedDataTypes;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.NullableAllPrimitiveDataTypes;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.SinglyNestedDataTypes;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.TimeContaining;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

/** Shared {@link SchemaAwareJavaBeans} data to be used across various tests. */
class FileWriteSchemaTransformFormatProviderTestData {
  static final FileWriteSchemaTransformFormatProviderTestData DATA =
      new FileWriteSchemaTransformFormatProviderTestData();

  /* Prevent instantiation outside this class. */
  private FileWriteSchemaTransformFormatProviderTestData() {}

  final List<AllPrimitiveDataTypes> allPrimitiveDataTypesList =
      Arrays.asList(
          allPrimitiveDataTypes(false, BigDecimal.valueOf(1L), 1.2345, 1.2345f, 1, 1L, "a"),
          allPrimitiveDataTypes(true, BigDecimal.valueOf(2L), 2.2345, 2.2345f, 2, 2L, "b"),
          allPrimitiveDataTypes(false, BigDecimal.valueOf(3L), 3.2345, 3.2345f, 3, 3L, "c"));

  final List<Row> allPrimitiveDataTypesRows =
      allPrimitiveDataTypesList.stream()
          .map(allPrimitiveDataTypesToRowFn()::apply)
          .collect(Collectors.toList());

  final List<NullableAllPrimitiveDataTypes> nullableAllPrimitiveDataTypesList =
      Arrays.asList(
          nullableAllPrimitiveDataTypes(null, null, null, null, null, null),
          nullableAllPrimitiveDataTypes(false, 1.2345, 1.2345f, 1, 1L, null),
          nullableAllPrimitiveDataTypes(false, 1.2345, 1.2345f, 1, null, "a"),
          nullableAllPrimitiveDataTypes(false, 1.2345, 1.2345f, null, 1L, "a"),
          nullableAllPrimitiveDataTypes(false, 1.2345, null, 1, 1L, "a"),
          nullableAllPrimitiveDataTypes(false, null, 1.2345f, 1, 1L, "a"),
          nullableAllPrimitiveDataTypes(null, 1.2345, 1.2345f, 1, 1L, "a"),
          nullableAllPrimitiveDataTypes(false, 1.2345, 1.2345f, 1, 1L, "a"));

  final List<Row> nullableAllPrimitiveDataTypesRows =
      nullableAllPrimitiveDataTypesList.stream()
          .map(nullableAllPrimitiveDataTypesToRowFn()::apply)
          .collect(Collectors.toList());

  final List<TimeContaining> timeContainingList =
      Arrays.asList(
          timeContaining(
              Instant.ofEpochMilli(1L), Collections.singletonList(Instant.ofEpochMilli(2L))),
          timeContaining(
              Instant.ofEpochMilli(Long.MAX_VALUE - 2L),
              Arrays.asList(Instant.ofEpochMilli(3L), Instant.ofEpochMilli(4L))));

  final List<Row> timeContainingRows =
      timeContainingList.stream().map(timeContainingToRowFn()::apply).collect(Collectors.toList());

  final List<ByteType> byteTypeList =
      Arrays.asList(
          byteType((byte) 1, Collections.singletonList((byte) 2)),
          byteType((byte) Byte.MAX_VALUE, Arrays.asList((byte) 3, (byte) 4)));

  final List<Row> byteTypeRows =
      byteTypeList.stream().map(byteTypeToRowFn()::apply).collect(Collectors.toList());

  final List<ByteSequenceType> byteSequenceTypeList =
      Arrays.asList(
          byteSequenceType(new byte[] {1, 2, 3}, Collections.singletonList(new byte[] {4, 5, 6})),
          byteSequenceType(
              new byte[] {Byte.MIN_VALUE, 0, Byte.MAX_VALUE},
              Arrays.asList(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, new byte[] {7, 8, 9})));

  final List<Row> byteSequenceTypeRows =
      byteSequenceTypeList.stream()
          .map(byteSequenceTypeToRowFn()::apply)
          .collect(Collectors.toList());

  final List<ArrayPrimitiveDataTypes> arrayPrimitiveDataTypesList =
      Arrays.asList(
          arrayPrimitiveDataTypes(
              Collections.singletonList(false),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList()),
          arrayPrimitiveDataTypes(
              Collections.emptyList(),
              Collections.singletonList(Double.MAX_VALUE),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList()),
          arrayPrimitiveDataTypes(
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.singletonList(Float.MAX_VALUE),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList()),
          arrayPrimitiveDataTypes(
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.singletonList(Integer.MAX_VALUE),
              Collections.emptyList(),
              Collections.emptyList()),
          arrayPrimitiveDataTypes(
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.singletonList(Long.MAX_VALUE),
              Collections.emptyList()),
          arrayPrimitiveDataTypes(
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.singletonList(
                  Stream.generate(() -> "🦄").limit(10).collect(Collectors.joining("")))),
          arrayPrimitiveDataTypes(
              Arrays.asList(false, true, false),
              Arrays.asList(Double.MIN_VALUE, 0.0, Double.MAX_VALUE),
              Arrays.asList(Float.MIN_VALUE, 0.0f, Float.MAX_VALUE),
              Arrays.asList(Integer.MIN_VALUE, 0, Integer.MAX_VALUE),
              Arrays.asList(Long.MIN_VALUE, 0L, Long.MAX_VALUE),
              Arrays.asList(
                  Stream.generate(() -> "🐤").limit(10).collect(Collectors.joining("")),
                  Stream.generate(() -> "🐥").limit(10).collect(Collectors.joining("")),
                  Stream.generate(() -> "🐣").limit(10).collect(Collectors.joining("")))),
          arrayPrimitiveDataTypes(
              Stream.generate(() -> true).limit(10).collect(Collectors.toList()),
              Stream.generate(() -> Double.MIN_VALUE).limit(10).collect(Collectors.toList()),
              Stream.generate(() -> Float.MIN_VALUE).limit(10).collect(Collectors.toList()),
              Stream.generate(() -> Integer.MIN_VALUE).limit(10).collect(Collectors.toList()),
              Stream.generate(() -> Long.MIN_VALUE).limit(10).collect(Collectors.toList()),
              Stream.generate(() -> "🐿").limit(10).collect(Collectors.toList())),
          arrayPrimitiveDataTypes(
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList()));

  final List<Row> arrayPrimitiveDataTypesRows =
      arrayPrimitiveDataTypesList.stream()
          .map(arrayPrimitiveDataTypesToRowFn()::apply)
          .collect(Collectors.toList());

  final List<SinglyNestedDataTypes> singlyNestedDataTypesNoRepeat =
      allPrimitiveDataTypesList.stream()
          .map(SchemaAwareJavaBeans::singlyNestedDataTypes)
          .collect(Collectors.toList());

  final List<Row> singlyNestedDataTypesNoRepeatRows =
      singlyNestedDataTypesNoRepeat.stream()
          .map(singlyNestedDataTypesToRowFn()::apply)
          .collect(Collectors.toList());

  final List<SinglyNestedDataTypes> singlyNestedDataTypesRepeated =
      allPrimitiveDataTypesList.stream()
          .map(
              (AllPrimitiveDataTypes element) ->
                  singlyNestedDataTypes(element, element, element, element))
          .collect(Collectors.toList());

  final List<Row> singlyNestedDataTypesRepeatedRows =
      singlyNestedDataTypesRepeated.stream()
          .map(singlyNestedDataTypesToRowFn()::apply)
          .collect(Collectors.toList());

  final List<DoublyNestedDataTypes> doublyNestedDataTypesNoRepeat =
      singlyNestedDataTypesNoRepeat.stream()
          .map(SchemaAwareJavaBeans::doublyNestedDataTypes)
          .collect(Collectors.toList());

  final List<Row> doublyNestedDataTypesNoRepeatRows =
      doublyNestedDataTypesNoRepeat.stream()
          .map(doublyNestedDataTypesToRowFn()::apply)
          .collect(Collectors.toList());

  final List<DoublyNestedDataTypes> doublyNestedDataTypesRepeat =
      singlyNestedDataTypesRepeated.stream()
          .map((SinglyNestedDataTypes element) -> doublyNestedDataTypes(element, element, element))
          .collect(Collectors.toList());

  final List<Row> doublyNestedDataTypesRepeatRows =
      doublyNestedDataTypesRepeat.stream()
          .map(doublyNestedDataTypesToRowFn()::apply)
          .collect(Collectors.toList());
}
