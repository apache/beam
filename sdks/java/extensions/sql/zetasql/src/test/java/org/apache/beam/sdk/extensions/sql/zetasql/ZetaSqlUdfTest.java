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
package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.zetasql.SqlException;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for user defined functions in the ZetaSQL dialect. */
@RunWith(JUnit4.class)
public class ZetaSqlUdfTest extends ZetaSqlTestBase {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    initialize();
  }

  @Test
  public void testAlreadyDefinedUDFThrowsException() {
    String sql = "CREATE FUNCTION foo() AS (0); CREATE FUNCTION foo() AS (1); SELECT foo();";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(ParseException.class);
    thrown.expectMessage("Failed to define function foo");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testNullaryUdf() {
    String sql = "CREATE FUNCTION zero() AS (0); SELECT zero();";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("x").build()).addValue(0L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testQualifiedNameUdfUnqualifiedCall() {
    String sql = "CREATE FUNCTION foo.bar.baz() AS (\"uwu\"); SELECT baz();";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("x").build()).addValue("uwu").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  @Ignore(
      "Qualified paths can't be resolved due to a bug in ZetaSQL: "
          + "https://github.com/google/zetasql/issues/42")
  public void testQualifiedNameUdfQualifiedCallThrowsException() {
    String sql = "CREATE FUNCTION foo.bar.baz() AS (\"uwu\"); SELECT foo.bar.baz();";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("x").build()).addValue("uwu").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUnaryUdf() {
    String sql = "CREATE FUNCTION triple(x INT64) AS (3 * x); SELECT triple(triple(1));";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("x").build()).addValue(9L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUdfWithinUdf() {
    String sql =
        "CREATE FUNCTION triple(x INT64) AS (3 * x);"
            + " CREATE FUNCTION nonuple(x INT64) as (triple(triple(x)));"
            + " SELECT nonuple(1);";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("x").build()).addValue(9L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUndefinedUdfThrowsException() {
    String sql =
        "CREATE FUNCTION foo() AS (bar()); "
            + "CREATE FUNCTION bar() AS (foo()); "
            + "SELECT foo();";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(SqlException.class);
    thrown.expectMessage("Function not found: bar");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testRecursiveUdfThrowsException() {
    String sql = "CREATE FUNCTION omega() AS (omega()); SELECT omega();";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(SqlException.class);
    thrown.expectMessage("Function not found: omega");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testUDTVF() {
    String sql =
        "CREATE TABLE FUNCTION CustomerRange(MinID INT64, MaxID INT64)\n"
            + "  AS\n"
            + "    SELECT *\n"
            + "    FROM KeyValue\n"
            + "    WHERE key >= MinId AND key <= MaxId; \n"
            + " SELECT key FROM CustomerRange(10, 14)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(singleField).addValues(14L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUDTVFTableNotFound() {
    String sql =
        "CREATE TABLE FUNCTION CustomerRange(MinID INT64, MaxID INT64)\n"
            + "  AS\n"
            + "    SELECT *\n"
            + "    FROM TableNotExist\n"
            + "    WHERE key >= MinId AND key <= MaxId; \n"
            + " SELECT key FROM CustomerRange(10, 14)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(SqlConversionException.class);
    thrown.expectMessage("Wasn't able to resolve the path [TableNotExist] in schema: beam");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testUDTVFFunctionNotFound() {
    String sql =
        "CREATE TABLE FUNCTION CustomerRange(MinID INT64, MaxID INT64)\n"
            + "  AS\n"
            + "    SELECT *\n"
            + "    FROM KeyValue\n"
            + "    WHERE key >= MinId AND key <= MaxId; \n"
            + " SELECT key FROM FunctionNotFound(10, 14)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(SqlException.class);
    thrown.expectMessage("Table-valued function not found: FunctionNotFound");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }
}
