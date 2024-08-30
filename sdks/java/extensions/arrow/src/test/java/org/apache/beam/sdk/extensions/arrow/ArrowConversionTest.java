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
package org.apache.beam.sdk.extensions.arrow;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ArrowConversionTest {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void teardown() {
    allocator.close();
  }

  @Test
  public void toBeamSchema_convertsSimpleArrowSchema() {
    Schema expected =
        Schema.of(Field.of("int8", FieldType.BYTE), Field.of("int16", FieldType.INT16));

    org.apache.arrow.vector.types.pojo.Schema arrowSchema =
        new org.apache.arrow.vector.types.pojo.Schema(
            ImmutableList.of(
                field("int8", new ArrowType.Int(8, true)),
                field("int16", new ArrowType.Int(16, true))));

    assertThat(ArrowConversion.ArrowSchemaTranslator.toBeamSchema(arrowSchema), equalTo(expected));
  }

  @Test
  public void rowIterator() {
    org.apache.arrow.vector.types.pojo.Schema schema =
        new org.apache.arrow.vector.types.pojo.Schema(
            asList(
                field("int32", new ArrowType.Int(32, true)),
                field("float64", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                field("string", new ArrowType.Utf8()),
                field("timestampMicroUTC", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
                field("timestampMilliUTC", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                field(
                    "int32_list",
                    new ArrowType.List(),
                    field("int32s", new ArrowType.Int(32, true))),
                field("boolean", new ArrowType.Bool()),
                field("fixed_size_binary", new ArrowType.FixedSizeBinary(3))));

    Schema beamSchema = ArrowConversion.ArrowSchemaTranslator.toBeamSchema(schema);

    VectorSchemaRoot expectedSchemaRoot = VectorSchemaRoot.create(schema, allocator);
    expectedSchemaRoot.allocateNew();
    expectedSchemaRoot.setRowCount(16);
    IntVector intVector = (IntVector) expectedSchemaRoot.getFieldVectors().get(0);
    Float8Vector floatVector = (Float8Vector) expectedSchemaRoot.getFieldVectors().get(1);
    VarCharVector strVector = (VarCharVector) expectedSchemaRoot.getFieldVectors().get(2);
    TimeStampMicroTZVector timestampMicroUtcVector =
        (TimeStampMicroTZVector) expectedSchemaRoot.getFieldVectors().get(3);
    TimeStampMilliTZVector timeStampMilliTZVector =
        (TimeStampMilliTZVector) expectedSchemaRoot.getFieldVectors().get(4);
    ListVector int32ListVector = (ListVector) expectedSchemaRoot.getFieldVectors().get(5);
    IntVector int32ListElementVector =
        int32ListVector
            .<IntVector>addOrGetVector(
                new org.apache.arrow.vector.types.pojo.FieldType(
                    false, new ArrowType.Int(32, true), null))
            .getVector();
    BitVector boolVector = (BitVector) expectedSchemaRoot.getFieldVectors().get(6);
    FixedSizeBinaryVector fixedSizeBinaryVector =
        (FixedSizeBinaryVector) expectedSchemaRoot.getFieldVectors().get(7);

    ArrayList<Row> expectedRows = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      DateTime dt = new DateTime(2019, 1, i + 1, i, i, i, DateTimeZone.UTC);
      expectedRows.add(
          Row.withSchema(beamSchema)
              .addValues(
                  i,
                  i + .1 * i,
                  "" + i,
                  dt,
                  dt,
                  ImmutableList.of(i),
                  (i % 2) != 0,
                  new byte[] {(byte) i, (byte) (i + 1), (byte) (i + 2)})
              .build());

      intVector.set(i, i);
      floatVector.set(i, i + .1 * i);
      strVector.set(i, new Text("" + i));
      timestampMicroUtcVector.set(i, dt.getMillis() * 1000);
      timeStampMilliTZVector.set(i, dt.getMillis());
      int32ListVector.startNewValue(i);
      int32ListElementVector.set(i, i);
      int32ListVector.endValue(i, 1);
      boolVector.set(i, i % 2);
      fixedSizeBinaryVector.set(i, new byte[] {(byte) i, (byte) (i + 1), (byte) (i + 2)});
    }

    assertThat(
        ImmutableList.copyOf(ArrowConversion.rowsFromRecordBatch(beamSchema, expectedSchemaRoot)),
        IsIterableContainingInOrder.contains(
            expectedRows.stream()
                .map((row) -> equalTo(row))
                .collect(ImmutableList.toImmutableList())));

    expectedSchemaRoot.close();
  }

  private static org.apache.arrow.vector.types.pojo.Field field(
      String name,
      boolean nullable,
      ArrowType type,
      org.apache.arrow.vector.types.pojo.Field... children) {
    return new org.apache.arrow.vector.types.pojo.Field(
        name,
        new org.apache.arrow.vector.types.pojo.FieldType(nullable, type, null, null),
        asList(children));
  }

  private static org.apache.arrow.vector.types.pojo.Field field(
      String name, ArrowType type, org.apache.arrow.vector.types.pojo.Field... children) {
    return field(name, false, type, children);
  }
}
