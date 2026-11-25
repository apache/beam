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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import org.apache.beam.sdk.jmh.schemas.RowBuilderBenchmark.RowState;
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
    assertThat(bench.buildComplexRow(state).getString("id"), equalTo("row-1"));
    assertThat(bench.fromRowSingleFlatOverride(state).getString("f_string"), equalTo("updated"));
    assertThat(
        bench.fromRowNestedOverride(state).getRow("child").getInt64("child_id"), equalTo(200L));
    assertThat(bench.nullRowCreation(state).getFieldCount(), equalTo(5));
    assertThat(bench.rowSorted(state).getFieldCount(), equalTo(10));
    assertThat(bench.rowToSnakeCase(state).getString("user_id"), equalTo("user-1"));
    assertThat(bench.verifyFieldValueString(), equalTo("beam-row-value"));
    assertThat(bench.parseSimpleFieldName(), notNullValue());
    assertThat(bench.parseDottedFieldPath(), notNullValue());
  }
}
