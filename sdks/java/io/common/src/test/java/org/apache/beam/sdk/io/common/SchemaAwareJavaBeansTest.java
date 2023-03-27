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
package org.apache.beam.sdk.io.common;

import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypesFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.arrayPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.arrayPrimitiveDataTypesFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.arrayPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.byteSequenceType;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.byteSequenceTypeFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.byteSequenceTypeToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.byteType;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.byteTypeFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.byteTypeToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.doublyNestedDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.doublyNestedDataTypesFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.doublyNestedDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypesFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.nullableAllPrimitiveDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.singlyNestedDataTypes;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.singlyNestedDataTypesFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.singlyNestedDataTypesToRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.timeContaining;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.timeContainingFromRowFn;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.timeContainingToRowFn;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SchemaAwareJavaBeans}. */
@RunWith(JUnit4.class)
public class SchemaAwareJavaBeansTest {
  @Test
  public void allPrimitiveDataTypesRowFns() {
    AllPrimitiveDataTypes element =
        allPrimitiveDataTypes(false, BigDecimal.valueOf(1L), 1.2345, 1.2345f, 1, 1L, "a");

    Row row = allPrimitiveDataTypesToRowFn().apply(element);
    assertEquals(element, allPrimitiveDataTypesFromRowFn().apply(row));
  }

  @Test
  public void nullableAllPrimitiveDataTypesRowFns() {
    NullableAllPrimitiveDataTypes allNull =
        nullableAllPrimitiveDataTypes(null, null, null, null, null, null);
    Row allNullRow = nullableAllPrimitiveDataTypesToRowFn().apply(allNull);
    assertEquals(allNull, nullableAllPrimitiveDataTypesFromRowFn().apply(allNullRow));

    NullableAllPrimitiveDataTypes nonNull =
        nullableAllPrimitiveDataTypes(false, 1.2345, 1.2345f, 1, 1L, "a");
    Row nonNullRow = nullableAllPrimitiveDataTypesToRowFn().apply(nonNull);
    assertEquals(nonNull, nullableAllPrimitiveDataTypesFromRowFn().apply(nonNullRow));
  }

  @Test
  public void timeContainingRowFns() {
    TimeContaining element =
        timeContaining(
            Instant.ofEpochMilli(1L),
            Arrays.asList(
                Instant.ofEpochMilli(2L), Instant.ofEpochMilli(3L), Instant.ofEpochMilli(4L)));
    Row row = timeContainingToRowFn().apply(element);
    assertEquals(element, timeContainingFromRowFn().apply(row));
  }

  @Test
  public void byteTypeRowFns() {
    ByteType element = byteType((byte) 1, Arrays.asList((byte) 1, (byte) 2, (byte) 3));

    Row row = byteTypeToRowFn().apply(element);
    assertEquals(element, byteTypeFromRowFn().apply(row));
  }

  @Test
  public void byteSequenceTypeRowFns() {
    ByteSequenceType element =
        byteSequenceType(
            new byte[] {1, 2, 3},
            Arrays.asList(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, new byte[] {7, 8, 9}));

    Row row = byteSequenceTypeToRowFn().apply(element);
    assertEquals(element, byteSequenceTypeFromRowFn().apply(row));
  }

  @Test
  public void arrayPrimitiveDataTypesRowFns() {
    ArrayPrimitiveDataTypes allEmpty =
        arrayPrimitiveDataTypes(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());
    Row allEmptyRow = arrayPrimitiveDataTypesToRowFn().apply(allEmpty);
    assertEquals(allEmpty, arrayPrimitiveDataTypesFromRowFn().apply(allEmptyRow));

    ArrayPrimitiveDataTypes noneEmpty =
        arrayPrimitiveDataTypes(
            Stream.generate(() -> true).limit(10).collect(Collectors.toList()),
            Stream.generate(() -> Double.MIN_VALUE).limit(10).collect(Collectors.toList()),
            Stream.generate(() -> Float.MIN_VALUE).limit(10).collect(Collectors.toList()),
            Stream.generate(() -> Integer.MIN_VALUE).limit(10).collect(Collectors.toList()),
            Stream.generate(() -> Long.MIN_VALUE).limit(10).collect(Collectors.toList()),
            Stream.generate(() -> "🐿").limit(10).collect(Collectors.toList()));
    Row noneEmptyRow = arrayPrimitiveDataTypesToRowFn().apply(noneEmpty);
    assertEquals(noneEmpty, arrayPrimitiveDataTypesFromRowFn().apply(noneEmptyRow));
  }

  @Test
  public void singlyNestedDataTypesRowFns() {
    AllPrimitiveDataTypes element =
        allPrimitiveDataTypes(false, BigDecimal.valueOf(1L), 1.2345, 1.2345f, 1, 1L, "a");
    SinglyNestedDataTypes notRepeated = singlyNestedDataTypes(element);
    SinglyNestedDataTypes repeated = singlyNestedDataTypes(element, element, element, element);
    Row notRepeatedRow = singlyNestedDataTypesToRowFn().apply(notRepeated);
    Row repeatedRow = singlyNestedDataTypesToRowFn().apply(repeated);
    assertEquals(notRepeated, singlyNestedDataTypesFromRowFn().apply(notRepeatedRow));
    assertEquals(repeated, singlyNestedDataTypesFromRowFn().apply(repeatedRow));
  }

  @Test
  public void doublyNestedDataTypesRowFns() {
    AllPrimitiveDataTypes element =
        allPrimitiveDataTypes(false, BigDecimal.valueOf(1L), 1.2345, 1.2345f, 1, 1L, "a");
    DoublyNestedDataTypes d0s0 = doublyNestedDataTypes(singlyNestedDataTypes(element));
    DoublyNestedDataTypes d1s0 =
        doublyNestedDataTypes(
            singlyNestedDataTypes(element),
            singlyNestedDataTypes(element),
            singlyNestedDataTypes(element),
            singlyNestedDataTypes(element));
    DoublyNestedDataTypes d0s1 =
        doublyNestedDataTypes(singlyNestedDataTypes(element, element, element, element));
    DoublyNestedDataTypes d1s1 =
        doublyNestedDataTypes(
            singlyNestedDataTypes(element, element, element, element),
            singlyNestedDataTypes(element, element, element, element),
            singlyNestedDataTypes(element, element, element, element),
            singlyNestedDataTypes(element, element, element, element));

    Row d0s0Row = doublyNestedDataTypesToRowFn().apply(d0s0);
    Row d1s0Row = doublyNestedDataTypesToRowFn().apply(d1s0);
    Row d0s1Row = doublyNestedDataTypesToRowFn().apply(d0s1);
    Row d1s1Row = doublyNestedDataTypesToRowFn().apply(d1s1);

    assertEquals(d0s0, doublyNestedDataTypesFromRowFn().apply(d0s0Row));
    assertEquals(d1s0, doublyNestedDataTypesFromRowFn().apply(d1s0Row));
    assertEquals(d0s1, doublyNestedDataTypesFromRowFn().apply(d0s1Row));
    assertEquals(d1s1, doublyNestedDataTypesFromRowFn().apply(d1s1Row));
  }
}
