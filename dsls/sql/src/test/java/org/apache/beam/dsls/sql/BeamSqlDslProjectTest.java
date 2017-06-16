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
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;

/**
 * Tests for field-project in queries.
 */
public class BeamSqlDslProjectTest extends BeamSqlDslBase {
  /**
   * select all fields.
   */
  @Test
  public void testSelectAll() throws Exception {
    String sql = "SELECT * FROM TABLE_A";

    PCollection<BeamSqlRow> result =
        inputA2.apply("testSelectAll", BeamSql.simpleQuery(sql));

    PAssert.that(result).containsInAnyOrder(recordsInTableA.get(0));

    pipeline.run().waitUntilFinish();
  }

  /**
   * select partial fields.
   */
  @Test
  public void testPartialFields() throws Exception {
    String sql = "SELECT f_int, f_long FROM TABLE_A";

    PCollection<BeamSqlRow> result =
        PCollectionTuple.of(new TupleTag<BeamSqlRow>("TABLE_A"), inputA2)
        .apply("testPartialFields", BeamSql.query(sql));

    BeamSqlRecordType resultType = new BeamSqlRecordType();
    resultType.addField("f_int", Types.INTEGER);
    resultType.addField("f_long", Types.BIGINT);

    BeamSqlRow record = new BeamSqlRow(resultType);
    record.addField("f_int", recordsInTableA.get(0).getFieldValue(0));
    record.addField("f_long", recordsInTableA.get(0).getFieldValue(1));

    PAssert.that(result).containsInAnyOrder(record);

    pipeline.run().waitUntilFinish();
  }

  /**
   * select partial fields for multiple rows.
   */
  @Test
  public void testPartialFieldsInMultipleRow() throws Exception {
    String sql = "SELECT f_int, f_long FROM TABLE_A";

    PCollection<BeamSqlRow> result =
        PCollectionTuple.of(new TupleTag<BeamSqlRow>("TABLE_A"), inputA1)
        .apply("testPartialFieldsInMultipleRow", BeamSql.query(sql));

    BeamSqlRecordType resultType = new BeamSqlRecordType();
    resultType.addField("f_int", Types.INTEGER);
    resultType.addField("f_long", Types.BIGINT);

    BeamSqlRow record1 = new BeamSqlRow(resultType);
    record1.addField("f_int", recordsInTableA.get(0).getFieldValue(0));
    record1.addField("f_long", recordsInTableA.get(0).getFieldValue(1));

    BeamSqlRow record2 = new BeamSqlRow(resultType);
    record2.addField("f_int", recordsInTableA.get(1).getFieldValue(0));
    record2.addField("f_long", recordsInTableA.get(1).getFieldValue(1));

    BeamSqlRow record3 = new BeamSqlRow(resultType);
    record3.addField("f_int", recordsInTableA.get(2).getFieldValue(0));
    record3.addField("f_long", recordsInTableA.get(2).getFieldValue(1));

    BeamSqlRow record4 = new BeamSqlRow(resultType);
    record4.addField("f_int", recordsInTableA.get(3).getFieldValue(0));
    record4.addField("f_long", recordsInTableA.get(3).getFieldValue(1));

    PAssert.that(result).containsInAnyOrder(record1, record2, record3, record4);

    pipeline.run().waitUntilFinish();
  }

  /**
   * select partial fields.
   */
  @Test
  public void testPartialFieldsInRows() throws Exception {
    String sql = "SELECT f_int, f_long FROM TABLE_A";

    PCollection<BeamSqlRow> result =
        PCollectionTuple.of(new TupleTag<BeamSqlRow>("TABLE_A"), inputA1)
        .apply("testPartialFieldsInRows", BeamSql.query(sql));

    BeamSqlRecordType resultType = new BeamSqlRecordType();
    resultType.addField("f_int", Types.INTEGER);
    resultType.addField("f_long", Types.BIGINT);

    BeamSqlRow record1 = new BeamSqlRow(resultType);
    record1.addField("f_int", recordsInTableA.get(0).getFieldValue(0));
    record1.addField("f_long", recordsInTableA.get(0).getFieldValue(1));

    BeamSqlRow record2 = new BeamSqlRow(resultType);
    record2.addField("f_int", recordsInTableA.get(1).getFieldValue(0));
    record2.addField("f_long", recordsInTableA.get(1).getFieldValue(1));

    BeamSqlRow record3 = new BeamSqlRow(resultType);
    record3.addField("f_int", recordsInTableA.get(2).getFieldValue(0));
    record3.addField("f_long", recordsInTableA.get(2).getFieldValue(1));

    BeamSqlRow record4 = new BeamSqlRow(resultType);
    record4.addField("f_int", recordsInTableA.get(3).getFieldValue(0));
    record4.addField("f_long", recordsInTableA.get(3).getFieldValue(1));

    PAssert.that(result).containsInAnyOrder(record1, record2, record3, record4);

    pipeline.run().waitUntilFinish();
  }

  /**
   * select literal field.
   */
  @Test
  public void testLiteralField() throws Exception {
    String sql = "SELECT 1 as literal_field FROM TABLE_A";

    PCollection<BeamSqlRow> result =
        PCollectionTuple.of(new TupleTag<BeamSqlRow>("TABLE_A"), inputA2)
        .apply("testLiteralField", BeamSql.query(sql));

    BeamSqlRecordType resultType = new BeamSqlRecordType();
    resultType.addField("literal_field", Types.INTEGER);

    BeamSqlRow record = new BeamSqlRow(resultType);
    record.addField("literal_field", 1);

    PAssert.that(result).containsInAnyOrder(record);

    pipeline.run().waitUntilFinish();
  }
}
