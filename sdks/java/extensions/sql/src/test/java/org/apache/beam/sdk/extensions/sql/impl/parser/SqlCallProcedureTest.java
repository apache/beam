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
package org.apache.beam.sdk.extensions.sql.impl.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog;
import org.apache.beam.sdk.extensions.sql.meta.catalog.CatalogRegistrar;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalog;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Procedure;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.CalciteContextException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@code CALL} procedure statements ({@link SqlCallProcedure}).
 *
 * <p>Uses a test-only {@link Catalog} that exposes a recording test procedure.
 */
public class SqlCallProcedureTest {
  private static final String TEST_CATALOG_TYPE = "test_with_procedures";

  private InMemoryCatalogManager catalogManager;
  private BeamSqlEnv env;

  @Before
  public void setUp() {
    TestProcedure.reset();
    NoOpProcedure.reset();
    catalogManager = new InMemoryCatalogManager();
    env =
        BeamSqlEnv.builder(catalogManager)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .build();
    env.executeDdl("CREATE CATALOG test_cat TYPE '" + TEST_CATALOG_TYPE + "'");
  }

  private void useTestCatalog() {
    env.executeDdl("USE CATALOG test_cat");
  }

  @Test
  public void testCallProcedure_positionalArgs() {
    useTestCatalog();
    env.executeDdl("CALL test_proc('db.tbl', 12345, true, 0.75, 'hello')");

    assertEquals(1, TestProcedure.executeCount.get());
    assertEquals(
        Row.withSchema(TestProcedure.PARAMETERS)
            .addValues("db.tbl", 12345L, true, 0.75, "hello")
            .build(),
        TestProcedure.lastArgs.get());
  }

  @Test
  public void testCallProcedure_positionalArgs_omitsTrailingOptional() {
    useTestCatalog();
    env.executeDdl("CALL test_proc('db.tbl', 42)");

    assertEquals(1, TestProcedure.executeCount.get());
    assertEquals(
        Row.withSchema(TestProcedure.PARAMETERS).addValues("db.tbl", 42L, null, null, null).build(),
        TestProcedure.lastArgs.get());
  }

  @Test
  public void testCallProcedure_namedArgs_anyOrder() {
    useTestCatalog();
    env.executeDdl("CALL test_proc(snapshot_id => 42, note => 'n', target => 'db.tbl')");

    assertEquals(1, TestProcedure.executeCount.get());
    assertEquals(
        Row.withSchema(TestProcedure.PARAMETERS).addValues("db.tbl", 42L, null, null, "n").build(),
        TestProcedure.lastArgs.get());
  }

  @Test
  public void testCallProcedure_emptyArgs() {
    useTestCatalog();
    env.executeDdl("CALL noop_proc()");

    assertEquals(1, NoOpProcedure.executeCount.get());
  }

  @Test
  public void testCallProcedure_negativeNumberArg() {
    useTestCatalog();
    env.executeDdl("CALL test_proc('db.tbl', -42)");

    Row args = TestProcedure.lastArgs.get();
    assertEquals(Long.valueOf(-42L), args.getInt64("snapshot_id"));
    assertEquals(1, TestProcedure.executeCount.get());
  }

  @Test
  public void testCallProcedure_nullForOptionalArg() {
    useTestCatalog();
    env.executeDdl("CALL test_proc('db.tbl', 1, NULL)");

    Row args = TestProcedure.lastArgs.get();
    assertNull(args.getBoolean("use_caching"));
    assertEquals(1, TestProcedure.executeCount.get());
  }

  @Test
  public void testCallProcedure_mixedArgs_error() {
    useTestCatalog();
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class,
            () -> env.executeDdl("CALL test_proc('db.tbl', snapshot_id => 42)"));
    assertThat(
        e.getMessage(), containsString("Mixing named and positional arguments is not supported"));
    assertEquals(0, TestProcedure.executeCount.get());
  }

  @Test
  public void testCallProcedure_missingRequiredArg_error() {
    useTestCatalog();
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class,
            () -> env.executeDdl("CALL test_proc(target => 'db.tbl')"));
    assertThat(e.getMessage(), containsString("Missing required argument(s)"));
    assertThat(e.getMessage(), containsString("snapshot_id"));
  }

  @Test
  public void testCallProcedure_unknownArgName_error() {
    useTestCatalog();
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class,
            () ->
                env.executeDdl(
                    "CALL test_proc(target => 'db.tbl', snapshot_id => 1, bad_param => 2)"));
    assertThat(e.getMessage(), containsString("does not accept an argument named 'bad_param'"));
  }

  @Test
  public void testCallProcedure_duplicateArgName_error() {
    useTestCatalog();
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class,
            () -> env.executeDdl("CALL test_proc(target => 'a', target => 'b', snapshot_id => 1)"));
    assertThat(e.getMessage(), containsString("Duplicate argument name 'target'"));
  }

  @Test
  public void testCallProcedure_tooManyArgs_error() {
    useTestCatalog();
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class,
            () -> env.executeDdl("CALL test_proc('a', 1, true, 0.5, 'n', 'extra')"));
    assertThat(e.getMessage(), containsString("Too many arguments"));
    assertThat(e.getMessage(), containsString("expected at most 5, got 6"));
  }

  @Test
  public void testCallProcedure_nonLiteralArg_error() {
    useTestCatalog();
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class, () -> env.executeDdl("CALL test_proc('db.tbl', 1 + 2)"));
    assertThat(e.getMessage(), containsString("must be a literal"));
  }

  @Test
  public void testCallProcedure_typeMismatch_error() {
    useTestCatalog();
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class,
            () -> env.executeDdl("CALL test_proc('db.tbl', 'not_a_number')"));
    assertThat(e.getMessage(), containsString("snapshot_id"));
    assertThat(e.getMessage(), containsString("INT64"));
  }

  @Test
  public void testCallProcedure_nullForRequiredArg_error() {
    useTestCatalog();
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class,
            () -> env.executeDdl("CALL test_proc(target => NULL, snapshot_id => 1)"));
    assertThat(e.getMessage(), containsString("'target'"));
    assertThat(e.getMessage(), containsString("cannot be NULL"));
  }

  @Test
  public void testCallProcedure_systemNamespace() {
    useTestCatalog();
    env.executeDdl("CALL system.test_proc('db.tbl', 7)");

    Row args = TestProcedure.lastArgs.get();
    assertEquals(Long.valueOf(7L), args.getInt64("snapshot_id"));
    assertEquals(1, TestProcedure.executeCount.get());
  }

  @Test
  public void testCallProcedure_fullyQualified() {
    // Current catalog remains 'default'; qualify the test catalog explicitly.
    env.executeDdl("CALL test_cat.system.test_proc('db.tbl', 99)");

    Row args = TestProcedure.lastArgs.get();
    assertEquals(Long.valueOf(99L), args.getInt64("snapshot_id"));
    assertEquals(1, TestProcedure.executeCount.get());
  }

  @Test
  public void testCallProcedure_caseInsensitiveResolution() {
    useTestCatalog();
    env.executeDdl("CALL SYSTEM.TEST_PROC('db.tbl', 3)");

    assertEquals(1, TestProcedure.executeCount.get());
    assertEquals(1, TestProcedure.executeCount.get());
  }

  @Test
  public void testCallProcedure_twoPartNonSystemNamespace_error() {
    useTestCatalog();
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class, () -> env.executeDdl("CALL foo.test_proc('db.tbl', 1)"));
    assertThat(e.getMessage(), containsString("Invalid procedure name 'foo.test_proc'"));
  }

  @Test
  public void testCallProcedure_threePartNonSystemNamespace_error() {
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class,
            () -> env.executeDdl("CALL test_cat.foo.test_proc('db.tbl', 1)"));
    assertThat(e.getMessage(), containsString("Invalid procedure name 'test_cat.foo.test_proc'"));
  }

  @Test
  public void testCallProcedure_tooManyNameParts_error() {
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class, () -> env.executeDdl("CALL a.b.c.d('db.tbl', 1)"));
    assertThat(e.getMessage(), containsString("Invalid procedure name 'a.b.c.d'"));
  }

  @Test
  public void testCallProcedure_unknownCatalog_error() {
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class,
            () -> env.executeDdl("CALL nope.system.test_proc('db.tbl', 1)"));
    assertThat(e.getMessage(), containsString("Catalog 'nope' not found"));
  }

  @Test
  public void testCallProcedure_unknownProcedure_error() {
    useTestCatalog();
    CalciteContextException e =
        assertThrows(CalciteContextException.class, () -> env.executeDdl("CALL nope_proc()"));
    assertThat(e.getMessage(), containsString("Procedure 'nope_proc' not found in catalog"));
    assertThat(e.getMessage(), containsString("test_cat"));
  }

  @Test
  public void testCallProcedure_catalogWithoutProcedureSupport_error() {
    // The default in-memory catalog provides no procedures.
    CalciteContextException e =
        assertThrows(
            CalciteContextException.class, () -> env.executeDdl("CALL test_proc('db.tbl', 1)"));
    assertThat(e.getMessage(), containsString("Procedure 'test_proc' not found in catalog"));
    assertThat(e.getMessage(), containsString("default"));
  }

  @Test
  public void testCallProcedure_missingParens_error() {
    useTestCatalog();
    assertThrows(ParseException.class, () -> env.executeDdl("CALL test_proc"));
  }

  @Test
  public void testCallProcedure_isDdl() {
    assertTrue(env.isDdl("CALL test_proc('db.tbl', 1)"));
  }

  @Test
  public void testCallProcedure_throughBeamSqlCli() {
    InMemoryCatalogManager cliCatalogManager = new InMemoryCatalogManager();
    BeamSqlCli cli = new BeamSqlCli().catalogManager(cliCatalogManager);
    cli.execute("CREATE CATALOG cli_cat TYPE '" + TEST_CATALOG_TYPE + "'");
    cli.execute("USE CATALOG cli_cat");
    cli.execute("CALL test_proc('db.tbl', 5)");

    assertEquals(1, TestProcedure.executeCount.get());
    Row args = TestProcedure.lastArgs.get();
    assertEquals(Long.valueOf(5L), args.getInt64("snapshot_id"));
  }

  @Test
  public void testUnparseCallProcedure() {
    SqlCallProcedure call =
        new SqlCallProcedure(
            SqlParserPos.ZERO,
            new SqlIdentifier(Arrays.asList("my_cat", "system", "test_proc"), SqlParserPos.ZERO),
            ImmutableList.of(
                SqlLiteral.createCharString("db.tbl", SqlParserPos.ZERO),
                SqlStdOperatorTable.ARGUMENT_ASSIGNMENT.createCall(
                    SqlParserPos.ZERO,
                    SqlLiteral.createExactNumeric("5", SqlParserPos.ZERO),
                    new SqlIdentifier("snapshot_id", SqlParserPos.ZERO))));

    SqlPrettyWriter sqlWriter =
        new SqlPrettyWriter(SqlPrettyWriter.config().withDialect(AnsiSqlDialect.DEFAULT));
    call.unparse(sqlWriter, 0, 0);
    assertEquals(
        "CALL `my_cat`.`system`.`test_proc`('db.tbl', `snapshot_id` => 5)",
        sqlWriter.toSqlString().getSql());
  }

  /**
   * Test-only catalog exposing procedures: a static registry map of procedure names to suppliers.
   */
  public static class TestProcedureCatalog extends InMemoryCatalog {
    private static final Map<String, Supplier<Procedure>> PROCEDURES =
        ImmutableMap.of(
            TestProcedure.NAME, TestProcedure::new,
            NoOpProcedure.NAME, NoOpProcedure::new);

    public TestProcedureCatalog(String name, Map<String, String> properties) {
      super(name, properties);
    }

    @Override
    public String type() {
      return TEST_CATALOG_TYPE;
    }

    @Override
    public @Nullable Procedure loadProcedure(String name) {
      Supplier<Procedure> supplier = PROCEDURES.get(name);
      return supplier == null ? null : supplier.get();
    }
  }

  /** Registers {@link TestProcedureCatalog} for {@code CREATE CATALOG ... TYPE}. */
  @AutoService(CatalogRegistrar.class)
  public static class TestProcedureCatalogRegistrar implements CatalogRegistrar {
    @Override
    public Iterable<Class<? extends Catalog>> getCatalogs() {
      return ImmutableList.of(TestProcedureCatalog.class);
    }
  }

  /** A recording test procedure with required and optional parameters. */
  private static class TestProcedure implements Procedure {
    static final String NAME = "test_proc";
    static final Schema PARAMETERS =
        Schema.builder()
            .addStringField("target")
            .addInt64Field("snapshot_id")
            .addNullableBooleanField("use_caching")
            .addNullableDoubleField("min_ratio")
            .addNullableStringField("note")
            .build();

    private static final AtomicReference<@Nullable Row> lastArgs = new AtomicReference<>();
    private static final AtomicInteger executeCount = new AtomicInteger();

    static void reset() {
      lastArgs.set(null);
      executeCount.set(0);
    }

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Schema parameters() {
      return PARAMETERS;
    }

    @Override
    public void execute(Row args) {
      lastArgs.set(args);
      executeCount.incrementAndGet();
    }
  }

  /** A recording test procedure with no parameters. */
  private static class NoOpProcedure implements Procedure {
    static final String NAME = "noop_proc";

    private static final AtomicInteger executeCount = new AtomicInteger();

    static void reset() {
      executeCount.set(0);
    }

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Schema parameters() {
      return Schema.builder().build();
    }

    @Override
    public void execute(Row args) {
      executeCount.incrementAndGet();
    }
  }
}
