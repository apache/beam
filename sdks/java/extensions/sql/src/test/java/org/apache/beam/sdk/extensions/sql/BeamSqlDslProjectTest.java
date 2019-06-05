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
package org.apache.beam.sdk.extensions.sql;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsString;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Assert;
import org.junit.Test;

/** Tests for field-project in queries with BOUNDED PCollection. */
public class BeamSqlDslProjectTest extends BeamSqlDslBase {
  /** select all fields with bounded PCollection. */
  @Test
  public void testSelectAllWithBounded() throws Exception {
    runSelectAll(boundedInput2);
  }

  /** select all fields with unbounded PCollection. */
  @Test
  public void testSelectAllWithUnbounded() throws Exception {
    runSelectAll(unboundedInput2);
  }

  private void runSelectAll(PCollection<Row> input) throws Exception {
    String sql = "SELECT * FROM PCOLLECTION";

    PCollection<Row> result = input.apply("testSelectAll", SqlTransform.query(sql));

    PAssert.that(result).containsInAnyOrder(rowsInTableA.get(0));

    pipeline.run();
  }

  /** select partial fields with bounded PCollection. */
  @Test
  public void testPartialFieldsWithBounded() throws Exception {
    runPartialFields(boundedInput2);
  }

  /** select partial fields with unbounded PCollection. */
  @Test
  public void testPartialFieldsWithUnbounded() throws Exception {
    runPartialFields(unboundedInput2);
  }

  private void runPartialFields(PCollection<Row> input) throws Exception {
    String sql = "SELECT f_int, f_long FROM TABLE_A";

    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), input)
            .apply("testPartialFields", SqlTransform.query(sql));

    Schema resultType = Schema.builder().addInt32Field("f_int").addInt64Field("f_long").build();

    Row row = rowAtIndex(resultType, 0);

    PAssert.that(result).containsInAnyOrder(row);

    pipeline.run();
  }

  /** select partial fields for multiple rows with bounded PCollection. */
  @Test
  public void testPartialFieldsInMultipleRowWithBounded() throws Exception {
    runPartialFieldsInMultipleRow(boundedInput1);
  }

  /** select partial fields for multiple rows with unbounded PCollection. */
  @Test
  public void testPartialFieldsInMultipleRowWithUnbounded() throws Exception {
    runPartialFieldsInMultipleRow(unboundedInput1);
  }

  private void runPartialFieldsInMultipleRow(PCollection<Row> input) throws Exception {
    String sql = "SELECT f_int, f_long FROM TABLE_A";

    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), input)
            .apply("testPartialFieldsInMultipleRow", SqlTransform.query(sql));

    Schema resultType = Schema.builder().addInt32Field("f_int").addInt64Field("f_long").build();

    List<Row> expectedRows =
        IntStream.range(0, 4).mapToObj(i -> rowAtIndex(resultType, i)).collect(toList());

    PAssert.that(result).containsInAnyOrder(expectedRows);

    pipeline.run();
  }

  private Row rowAtIndex(Schema schema, int index) {
    return Row.withSchema(schema)
        .addValues(rowsInTableA.get(index).getValue(0), rowsInTableA.get(index).getValue(1))
        .build();
  }

  /** select partial fields with bounded PCollection. */
  @Test
  public void testPartialFieldsInRowsWithBounded() throws Exception {
    runPartialFieldsInRows(boundedInput1);
  }

  /** select partial fields with unbounded PCollection. */
  @Test
  public void testPartialFieldsInRowsWithUnbounded() throws Exception {
    runPartialFieldsInRows(unboundedInput1);
  }

  private void runPartialFieldsInRows(PCollection<Row> input) throws Exception {
    String sql = "SELECT f_int, f_long FROM TABLE_A";

    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), input)
            .apply("testPartialFieldsInRows", SqlTransform.query(sql));

    Schema resultType = Schema.builder().addInt32Field("f_int").addInt64Field("f_long").build();

    List<Row> expectedRows =
        IntStream.range(0, 4).mapToObj(i -> rowAtIndex(resultType, i)).collect(toList());

    PAssert.that(result).containsInAnyOrder(expectedRows);

    pipeline.run();
  }

  /** select literal field with bounded PCollection. */
  @Test
  public void testLiteralFieldWithBounded() throws Exception {
    runLiteralField(boundedInput2);
  }

  /** select literal field with unbounded PCollection. */
  @Test
  public void testLiteralFieldWithUnbounded() throws Exception {
    runLiteralField(unboundedInput2);
  }

  public void runLiteralField(PCollection<Row> input) throws Exception {
    String sql = "SELECT 1 as literal_field FROM TABLE_A";

    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), input)
            .apply("testLiteralField", SqlTransform.query(sql));

    Schema resultType = Schema.builder().addInt32Field("literal_field").build();

    Row row = Row.withSchema(resultType).addValues(1).build();

    PAssert.that(result).containsInAnyOrder(row);

    pipeline.run();
  }

  @Test
  public void testProjectUnknownField() throws Exception {
    exceptions.expect(ParseException.class);
    exceptions.expectCause(hasMessage(containsString("Column 'f_int_na' not found in any table")));
    pipeline.enableAbandonedNodeEnforcement(false);

    String sql = "SELECT f_int_na FROM TABLE_A";

    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), boundedInput1)
            .apply("testProjectUnknownField", SqlTransform.query(sql));

    pipeline.run();
  }

  /**
   * Trivial programs project precisely their input fields, without dropping or re-ordering them.
   *
   * @see <a href="https://issues.apache.org/jira/browse/BEAM-6810">BEAM-6810</a>
   */
  @Test
  public void testTrivialProjection() {
    String sql = "SELECT c_int64 as abc FROM PCOLLECTION";
    Schema inputSchema = Schema.of(Schema.Field.of("c_int64", Schema.FieldType.INT64));
    Schema outputSchema = Schema.of(Schema.Field.of("abc", Schema.FieldType.INT64));

    PCollection<Row> input =
        pipeline.apply(
            Create.of(Row.withSchema(inputSchema).addValue(42L).build())
                .withRowSchema(inputSchema));

    PCollection<Row> result = input.apply(SqlTransform.query(sql));

    Assert.assertEquals(outputSchema, result.getSchema());

    PAssert.that(result).containsInAnyOrder(Row.withSchema(outputSchema).addValue(42L).build());

    pipeline.run();
  }
}
