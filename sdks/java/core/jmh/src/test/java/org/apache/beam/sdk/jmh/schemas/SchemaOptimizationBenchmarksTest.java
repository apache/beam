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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.jmh.schemas.RowAndSchemaBenchmark.RowState;
import org.apache.beam.sdk.jmh.schemas.SchemaCoderBenchmark.CoderState;
import org.apache.beam.sdk.jmh.schemas.SchemaTransformsBenchmark.TransformState;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Smoke tests verifying all schema JMH benchmark methods run cleanly and return expected output.
 */
@RunWith(JUnit4.class)
public class SchemaOptimizationBenchmarksTest {

  @Test
  public void testSchemaCoderBenchmark() throws Exception {
    SchemaCoderBenchmark bench = new SchemaCoderBenchmark();
    CoderState s = new CoderState();
    s.setUp();

    assertThat(bench.rowCoderFlatEncode(s), greaterThan(0));
    assertThat(bench.rowCoderFlatDecode(s), equalTo(s.flatRow));
    assertThat(bench.rowCoderFlatByteSize(s), greaterThan(0L));
    assertThat(bench.rowCoderStaticEncode(s), greaterThan(0));
    assertThat(bench.rowCoderStaticDecode(s), equalTo(s.staticRow));
    assertThat(bench.rowCoderNullableNoNullsEncode(s), greaterThan(0));
    assertThat(bench.rowCoderNullableNoNullsDecode(s), equalTo(s.nullableNoNullsRow));
    assertThat(bench.rowCoderNullableSparseEncode(s), greaterThan(0));
    assertThat(bench.rowCoderNullableSparseDecode(s), equalTo(s.nullableSparseNullsRow));
    assertThat(bench.rowCoderWide40Encode(s), greaterThan(0));
    assertThat(bench.rowCoderWide40Decode(s), equalTo(s.wideRow));
    assertThat(bench.rowCoderNestedEncode(s), greaterThan(0));
    assertThat(bench.rowCoderNestedDecode(s), equalTo(s.nestedRow));
    assertThat(bench.rowCoderLogicalTypesEncode(s), greaterThan(0));
    assertThat(bench.rowCoderLogicalTypesDecode(s), equalTo(s.logicalRow));
    assertThat(bench.rowCoderLazyIterableByteSize(s), equalTo(1L));
    assertThat(bench.schemaCoderFlatPojoEncode(s), greaterThan(0));
    assertThat(bench.schemaCoderFlatPojoDecode(s).fString1, equalTo("us-central1"));
    assertThat(bench.schemaCoderRichPojoEncode(s), greaterThan(0));
    assertThat(bench.schemaCoderRichPojoDecode(s).name, equalTo("widget"));
    assertThat(bench.schemaCoderJavaBeanEncode(s), greaterThan(0));
    assertThat(bench.schemaCoderJavaBeanDecode(s).getFString1(), equalTo("us-central1"));
  }

  @Test
  public void testRowAndSchemaBenchmark() throws Exception {
    RowAndSchemaBenchmark bench = new RowAndSchemaBenchmark();
    RowState s = new RowState();
    s.setUp();

    Blackhole bh =
        new Blackhole(
            "Today's password is swordfish. I understand instantiating Blackholes directly is dangerous.");
    assertTrue(bench.rowEqualsDifferentSchemaUuid(s));
    assertThat(bench.rowHashCode(s), equalTo(s.rowStorageA.hashCode()));
    assertThat(bench.rowGetDateTime(s), notNullValue());
    assertThat(bench.rowGetBaseValues(s).size(), equalTo(7));
    bench.rowWithGettersCachedCollectionRead(s, bh);
    bench.rowWithGettersFreshReadAll(s, bh);
    assertThat(bench.rowWithGettersNestedRowRead(s), equalTo("Seattle"));
    assertThat(bench.flatPojoFromRow(s).fString1, equalTo("alpha"));
    assertThat(bench.richPojoFromRow(s).name, equalTo("widget"));
    assertThat(bench.javaBeanFromRow(s).getFString1(), equalTo("alpha"));
    assertTrue(bench.schemaEqualsDifferentUuid(s));
    assertTrue(bench.schemaEquivalent(s));
    assertThat(bench.schemaNameOf(s), equalTo("amount"));
    assertThat(bench.schemaGetFieldNames(s).size(), equalTo(7));
    bench.typeNamePredicates(s);
    assertThat(bench.rowMessagesToBytes(s).length, greaterThan(0));
    assertThat(bench.jsonRowToBytes(s).length, greaterThan(0));
    assertThat(bench.jsonBytesToRow(s), equalTo(s.jsonRow));
    assertThat(bench.logicalTimestampToInput(s), equalTo(s.javaInstant));
    assertThat(bench.logicalTimestampToBase(s).getFieldCount(), equalTo(2));
    assertThat(bench.logicalDateTimeToInput(s), equalTo(s.ldt));
    assertThat(bench.logicalDateTimeToBase(s).getFieldCount(), equalTo(2));
    assertThat(bench.logicalUuidToInput(s), equalTo(s.uuid));
    assertThat(bench.logicalUuidToBase(s).getFieldCount(), equalTo(2));
    assertThat(bench.logicalEnumValueOf(s).getValue(), equalTo(2));
    assertThat(bench.logicalOneOfToInput(s), equalTo(s.oneOfVal));
    assertThat(bench.logicalOneOfToBase(s).getFieldCount(), equalTo(4));
    assertThat(bench.logicalFixedNumericToInput(s), equalTo(s.numericVal));
  }

  @Test
  public void testSchemaTransformsBenchmark() {
    SchemaTransformsBenchmark bench = new SchemaTransformsBenchmark();
    TransformState s = new TransformState();
    s.setUp();

    assertThat(bench.rowSelectorWildcard30(s).getFieldCount(), equalTo(30));
    assertThat(bench.rowSelectorSubset3Of30(s).getFieldCount(), equalTo(3));
    assertThat(bench.rowSelectorNestedList16(s).getFieldCount(), equalTo(3));
    assertThat(bench.rowSelectorNestedMap16(s).getFieldCount(), equalTo(3));
    Row agg = bench.schemaAggregateFn16Rows(s);
    assertThat(agg.getInt64("total_cost"), equalTo(1360L));
    assertThat(agg.getInt64("bid_count"), equalTo(16L));
    assertThat(bench.schemaAggregateFnMerge4(s), notNullValue());
    assertThat(bench.castRowIdenticalSchema(s).getFieldCount(), equalTo(5));
    assertThat(bench.castRowWidenedSchema(s).getInt64("id"), equalTo(42L));
  }
}
