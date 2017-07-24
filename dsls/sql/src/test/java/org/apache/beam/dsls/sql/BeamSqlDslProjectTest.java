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
package org.apache.beam.dsls.sql;

import java.sql.Types;
import java.util.Arrays;
import org.apache.beam.sdk.sd.BeamRow;
import org.apache.beam.sdk.sd.BeamRowType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;

/**
 * Tests for field-project in queries with BOUNDED PCollection.
 */
public class BeamSqlDslProjectTest extends BeamSqlDslBase {
  /**
   * select all fields with bounded PCollection.
   */
  @Test
  public void testSelectAllWithBounded() throws Exception {
    runSelectAll(boundedInput2);
  }

  /**
   * select all fields with unbounded PCollection.
   */
  @Test
  public void testSelectAllWithUnbounded() throws Exception {
    runSelectAll(unboundedInput2);
  }

  private void runSelectAll(PCollection<BeamRow> input) throws Exception {
    String sql = "SELECT * FROM PCOLLECTION";

    PCollection<BeamRow> result =
        input.apply("testSelectAll", BeamSql.simpleQuery(sql));

    PAssert.that(result).containsInAnyOrder(recordsInTableA.get(0));

    pipeline.run().waitUntilFinish();
  }

  /**
   * select partial fields with bounded PCollection.
   */
  @Test
  public void testPartialFieldsWithBounded() throws Exception {
    runPartialFields(boundedInput2);
  }

  /**
   * select partial fields with unbounded PCollection.
   */
  @Test
  public void testPartialFieldsWithUnbounded() throws Exception {
    runPartialFields(unboundedInput2);
  }

  private void runPartialFields(PCollection<BeamRow> input) throws Exception {
    String sql = "SELECT f_int, f_long FROM TABLE_A";

    PCollection<BeamRow> result =
        PCollectionTuple.of(new TupleTag<BeamRow>("TABLE_A"), input)
        .apply("testPartialFields", BeamSql.query(sql));

    BeamRowType resultType = BeamRowType.create(Arrays.asList("f_int", "f_long"),
        Arrays.asList(Types.INTEGER, Types.BIGINT));

    BeamRow record = new BeamRow(resultType);
    record.addField("f_int", recordsInTableA.get(0).getFieldValue(0));
    record.addField("f_long", recordsInTableA.get(0).getFieldValue(1));

    PAssert.that(result).containsInAnyOrder(record);

    pipeline.run().waitUntilFinish();
  }

  /**
   * select partial fields for multiple rows with bounded PCollection.
   */
  @Test
  public void testPartialFieldsInMultipleRowWithBounded() throws Exception {
    runPartialFieldsInMultipleRow(boundedInput1);
  }

  /**
   * select partial fields for multiple rows with unbounded PCollection.
   */
  @Test
  public void testPartialFieldsInMultipleRowWithUnbounded() throws Exception {
    runPartialFieldsInMultipleRow(unboundedInput1);
  }

  private void runPartialFieldsInMultipleRow(PCollection<BeamRow> input) throws Exception {
    String sql = "SELECT f_int, f_long FROM TABLE_A";

    PCollection<BeamRow> result =
        PCollectionTuple.of(new TupleTag<BeamRow>("TABLE_A"), input)
        .apply("testPartialFieldsInMultipleRow", BeamSql.query(sql));

    BeamRowType resultType = BeamRowType.create(Arrays.asList("f_int", "f_long"),
        Arrays.asList(Types.INTEGER, Types.BIGINT));

    BeamRow record1 = new BeamRow(resultType);
    record1.addField("f_int", recordsInTableA.get(0).getFieldValue(0));
    record1.addField("f_long", recordsInTableA.get(0).getFieldValue(1));

    BeamRow record2 = new BeamRow(resultType);
    record2.addField("f_int", recordsInTableA.get(1).getFieldValue(0));
    record2.addField("f_long", recordsInTableA.get(1).getFieldValue(1));

    BeamRow record3 = new BeamRow(resultType);
    record3.addField("f_int", recordsInTableA.get(2).getFieldValue(0));
    record3.addField("f_long", recordsInTableA.get(2).getFieldValue(1));

    BeamRow record4 = new BeamRow(resultType);
    record4.addField("f_int", recordsInTableA.get(3).getFieldValue(0));
    record4.addField("f_long", recordsInTableA.get(3).getFieldValue(1));

    PAssert.that(result).containsInAnyOrder(record1, record2, record3, record4);

    pipeline.run().waitUntilFinish();
  }

  /**
   * select partial fields with bounded PCollection.
   */
  @Test
  public void testPartialFieldsInRowsWithBounded() throws Exception {
    runPartialFieldsInRows(boundedInput1);
  }

  /**
   * select partial fields with unbounded PCollection.
   */
  @Test
  public void testPartialFieldsInRowsWithUnbounded() throws Exception {
    runPartialFieldsInRows(unboundedInput1);
  }

  private void runPartialFieldsInRows(PCollection<BeamRow> input) throws Exception {
    String sql = "SELECT f_int, f_long FROM TABLE_A";

    PCollection<BeamRow> result =
        PCollectionTuple.of(new TupleTag<BeamRow>("TABLE_A"), input)
        .apply("testPartialFieldsInRows", BeamSql.query(sql));

    BeamRowType resultType = BeamRowType.create(Arrays.asList("f_int", "f_long"),
        Arrays.asList(Types.INTEGER, Types.BIGINT));

    BeamRow record1 = new BeamRow(resultType);
    record1.addField("f_int", recordsInTableA.get(0).getFieldValue(0));
    record1.addField("f_long", recordsInTableA.get(0).getFieldValue(1));

    BeamRow record2 = new BeamRow(resultType);
    record2.addField("f_int", recordsInTableA.get(1).getFieldValue(0));
    record2.addField("f_long", recordsInTableA.get(1).getFieldValue(1));

    BeamRow record3 = new BeamRow(resultType);
    record3.addField("f_int", recordsInTableA.get(2).getFieldValue(0));
    record3.addField("f_long", recordsInTableA.get(2).getFieldValue(1));

    BeamRow record4 = new BeamRow(resultType);
    record4.addField("f_int", recordsInTableA.get(3).getFieldValue(0));
    record4.addField("f_long", recordsInTableA.get(3).getFieldValue(1));

    PAssert.that(result).containsInAnyOrder(record1, record2, record3, record4);

    pipeline.run().waitUntilFinish();
  }

  /**
   * select literal field with bounded PCollection.
   */
  @Test
  public void testLiteralFieldWithBounded() throws Exception {
    runLiteralField(boundedInput2);
  }

  /**
   * select literal field with unbounded PCollection.
   */
  @Test
  public void testLiteralFieldWithUnbounded() throws Exception {
    runLiteralField(unboundedInput2);
  }

  public void runLiteralField(PCollection<BeamRow> input) throws Exception {
    String sql = "SELECT 1 as literal_field FROM TABLE_A";

    PCollection<BeamRow> result =
        PCollectionTuple.of(new TupleTag<BeamRow>("TABLE_A"), input)
        .apply("testLiteralField", BeamSql.query(sql));

    BeamRowType resultType = BeamRowType.create(Arrays.asList("literal_field"),
        Arrays.asList(Types.INTEGER));

    BeamRow record = new BeamRow(resultType);
    record.addField("literal_field", 1);

    PAssert.that(result).containsInAnyOrder(record);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testProjectUnknownField() throws Exception {
    exceptions.expect(IllegalStateException.class);
    exceptions.expectMessage("Column 'f_int_na' not found in any table");
    pipeline.enableAbandonedNodeEnforcement(false);

    String sql = "SELECT f_int_na FROM TABLE_A";

    PCollection<BeamRow> result =
        PCollectionTuple.of(new TupleTag<BeamRow>("TABLE_A"), boundedInput1)
        .apply("testProjectUnknownField", BeamSql.query(sql));

    pipeline.run().waitUntilFinish();
  }
}
