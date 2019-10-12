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
package org.apache.beam.sdk.util;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.utils.ArrowUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ArrowUtilsTest {

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

    org.apache.arrow.vector.types.pojo.Schema arrow_schema =
        new org.apache.arrow.vector.types.pojo.Schema(
            ImmutableList.of(
                field("int8", new ArrowType.Int(8, true)),
                field("int16", new ArrowType.Int(16, true))));

    assertThat(ArrowUtils.toBeamSchema(arrow_schema), equalTo(expected));
  }

  @Test
  public void rowIterator() {
    org.apache.arrow.vector.types.pojo.Schema schema =
        new org.apache.arrow.vector.types.pojo.Schema(
            asList(
                field("int32", new ArrowType.Int(32, true)),
                field("float64", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
                // field("string", new ArrowType.Utf8())
                ));

    Schema beamSchema = ArrowUtils.toBeamSchema(schema);

    VectorSchemaRoot expectedSchemaRoot = VectorSchemaRoot.create(schema, allocator);
    expectedSchemaRoot.setRowCount(16);
    for (FieldVector vector : expectedSchemaRoot.getFieldVectors()) {
      vector.allocateNew();
    }
    IntVector intVector = (IntVector) expectedSchemaRoot.getFieldVectors().get(0);
    Float8Vector floatVector = (Float8Vector) expectedSchemaRoot.getFieldVectors().get(1);
    // VarCharVector strVector = (VarCharVector)expectedSchemaRoot.getFieldVectors().get(2);

    ArrayList<Row> expectedRows = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      expectedRows.add(Row.withSchema(beamSchema).addValues(i, i + .1 * i).build());
      intVector.set(i, i);
      floatVector.set(i, i + .1 * i);
      // strVector.set(i, new Text("" + i));
    }

    Iterable<Row> rowIterator = ArrowUtils.rowsFromRecordBatch(beamSchema, expectedSchemaRoot);
    for (Row row : rowIterator) {
      System.out.println(row);
    }

    assertThat(
        ArrowUtils.rowsFromRecordBatch(beamSchema, expectedSchemaRoot),
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
