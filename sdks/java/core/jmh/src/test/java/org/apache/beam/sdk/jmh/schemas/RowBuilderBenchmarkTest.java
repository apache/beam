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
package org.apache.beam.sdk.jmh.schemas;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import org.apache.beam.sdk.jmh.schemas.RowBuilderBenchmark.RowState;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Smoke tests for {@link RowBuilderBenchmark}. */
@RunWith(JUnit4.class)
public class RowBuilderBenchmarkTest {

  @Test
  public void testAllBenchmarks() {
    RowBuilderBenchmark bench = new RowBuilderBenchmark();
    RowState state = new RowState();
    state.setUp();

    Row flat = bench.buildFlatPrimitiveRow(state);
    assertThat(bench.buildFlatNamedMapRow(state), equalTo(flat));
    assertThat(bench.buildFlatByFieldName(state), equalTo(flat));
    assertThat(bench.buildFlatByFieldId(state), equalTo(flat));
    assertThat(bench.rowToRowCollector(state), equalTo(flat));
    assertThat(bench.buildComplexRow(state).getString("id"), equalTo("row-1"));
    assertThat(bench.fromRowSingleFlatOverride(state).getString("f_string"), equalTo("updated"));
    assertThat(
        bench.fromRowNestedOverride(state).getRow("child").getInt64("child_id"), equalTo(200L));
    assertThat(
        bench.fromRowDeepNestedOverride(state).getRow("outer").getRow("child").getInt64("child_id"),
        equalTo(200L));

    Row nullRow = bench.nullRowCreation(state);
    assertThat(nullRow.getFieldCount(), equalTo(5));
    assertThat(nullRow.getValue(0), nullValue());

    assertThat(bench.rowGetBaseValues(state).size(), equalTo(10));
    assertThat(bench.rowToPrettyString(state), containsString("f_string: \"beam-row-value\""));

    Row sorted = bench.rowSorted(state);
    assertThat(sorted.getFieldCount(), equalTo(10));
    assertThat(sorted.getSchema().getField(0).getName(), equalTo("f_bool"));
    assertThat(bench.rowToSnakeCase(state).getString("user_id"), equalTo("user-1"));
    assertThat(bench.buildByteBufferSliceRow(state).getBytes(0), equalTo(new byte[] {1, 2, 3, 4}));
    assertThat(bench.verifyFieldValueString(state), equalTo("beam-row-value"));

    assertThat(
        bench.parseSimpleFieldName(state),
        equalTo(
            FieldAccessDescriptor.withFields(
                FieldDescriptor.builder().setFieldName("f_string").build())));
    assertThat(
        bench.parseDottedFieldPath(state),
        equalTo(
            FieldAccessDescriptor.create()
                .withNestedField(
                    FieldDescriptor.builder().setFieldName("outer").build(),
                    FieldAccessDescriptor.create()
                        .withNestedField(
                            FieldDescriptor.builder().setFieldName("child").build(),
                            FieldAccessDescriptor.withFields(
                                FieldDescriptor.builder().setFieldName("child_id").build())))));
    assertThat(
        bench.parseDottedWildcardPath(state),
        equalTo(
            FieldAccessDescriptor.create()
                .withNestedField(
                    FieldDescriptor.builder().setFieldName("outer").build(),
                    FieldAccessDescriptor.create()
                        .withNestedField(
                            FieldDescriptor.builder().setFieldName("child").build(),
                            FieldAccessDescriptor.withAllFields()))));
  }
}
