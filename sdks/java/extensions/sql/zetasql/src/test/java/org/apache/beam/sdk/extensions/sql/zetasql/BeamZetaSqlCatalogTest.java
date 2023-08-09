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

import static org.apache.beam.sdk.extensions.sql.zetasql.BeamZetaSqlCatalog.USER_DEFINED_JAVA_SCALAR_FUNCTIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.zetasql.Analyzer;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.resolvedast.ResolvedNodes;
import java.lang.reflect.Method;
import java.sql.Time;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.ScalarFunctionImpl;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.UserFunctionDefinitions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamZetaSqlCatalog}. */
@RunWith(JUnit4.class)
public class BeamZetaSqlCatalogTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  public static class IncrementFn implements BeamSqlUdf {
    public Long eval(Long i) {
      return i + 1;
    }
  }

  public static class ReturnsArrayTimeFn implements BeamSqlUdf {
    public List<Time> eval() {
      return ImmutableList.of(new Time(0));
    }
  }

  public static class TakesArrayTimeFn implements BeamSqlUdf {
    public Long eval(List<Time> ls) {
      return 0L;
    }
  }

  @Test
  public void loadsUserDefinedFunctionsFromSchema() throws NoSuchMethodException {
    JdbcConnection jdbcConnection = createJdbcConnection();
    SchemaPlus calciteSchema = jdbcConnection.getCurrentSchemaPlus();
    Method method = IncrementFn.class.getMethod("eval", Long.class);
    calciteSchema.add("increment", ScalarFunctionImpl.create(method));
    BeamZetaSqlCatalog beamCatalog =
        BeamZetaSqlCatalog.create(
            calciteSchema, jdbcConnection.getTypeFactory(), SqlAnalyzer.baseAnalyzerOptions());
    assertNotNull(
        "ZetaSQL catalog contains function signature.",
        beamCatalog
            .getZetaSqlCatalog()
            .getFunctionByFullName(USER_DEFINED_JAVA_SCALAR_FUNCTIONS + ":increment"));
    assertEquals(
        "Beam catalog contains function definition.",
        UserFunctionDefinitions.JavaScalarFunction.create(method, ""),
        beamCatalog
            .getUserFunctionDefinitions()
            .javaScalarFunctions()
            .get(ImmutableList.of("increment")));
  }

  @Test
  public void rejectsScalarFunctionImplWithUnsupportedReturnType() throws NoSuchMethodException {
    JdbcConnection jdbcConnection = createJdbcConnection();
    SchemaPlus calciteSchema = jdbcConnection.getCurrentSchemaPlus();
    Method method = ReturnsArrayTimeFn.class.getMethod("eval");
    calciteSchema.add("return_array", ScalarFunctionImpl.create(method));
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Calcite type TIME not allowed in function return_array");
    BeamZetaSqlCatalog.create(
        calciteSchema, jdbcConnection.getTypeFactory(), SqlAnalyzer.baseAnalyzerOptions());
  }

  @Test
  public void rejectsScalarFunctionImplWithUnsupportedParameterType() throws NoSuchMethodException {
    JdbcConnection jdbcConnection = createJdbcConnection();
    SchemaPlus calciteSchema = jdbcConnection.getCurrentSchemaPlus();
    Method method = TakesArrayTimeFn.class.getMethod("eval", List.class);
    calciteSchema.add("take_array", ScalarFunctionImpl.create(method));
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Calcite type TIME not allowed in function take_array");
    BeamZetaSqlCatalog.create(
        calciteSchema, jdbcConnection.getTypeFactory(), SqlAnalyzer.baseAnalyzerOptions());
  }

  @Test
  public void rejectsCreateFunctionStmtWithUnsupportedReturnType() {
    JdbcConnection jdbcConnection = createJdbcConnection();
    AnalyzerOptions analyzerOptions = SqlAnalyzer.baseAnalyzerOptions();
    BeamZetaSqlCatalog beamCatalog =
        BeamZetaSqlCatalog.create(
            jdbcConnection.getCurrentSchemaPlus(),
            jdbcConnection.getTypeFactory(),
            analyzerOptions);

    String sql =
        "CREATE FUNCTION foo() RETURNS ARRAY<TIME> LANGUAGE java OPTIONS (path='/does/not/exist');";
    ResolvedNodes.ResolvedStatement resolvedStatement =
        Analyzer.analyzeStatement(sql, analyzerOptions, beamCatalog.getZetaSqlCatalog());
    ResolvedNodes.ResolvedCreateFunctionStmt createFunctionStmt =
        (ResolvedNodes.ResolvedCreateFunctionStmt) resolvedStatement;

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("ZetaSQL type TYPE_TIME not allowed in function foo");
    beamCatalog.addFunction(createFunctionStmt);
  }

  @Test
  public void rejectsCreateFunctionStmtWithUnsupportedParameterType() {
    JdbcConnection jdbcConnection = createJdbcConnection();
    AnalyzerOptions analyzerOptions = SqlAnalyzer.baseAnalyzerOptions();
    BeamZetaSqlCatalog beamCatalog =
        BeamZetaSqlCatalog.create(
            jdbcConnection.getCurrentSchemaPlus(),
            jdbcConnection.getTypeFactory(),
            analyzerOptions);

    String sql =
        "CREATE FUNCTION foo(a ARRAY<TIME>) RETURNS INT64 LANGUAGE java OPTIONS (path='/does/not/exist');";
    ResolvedNodes.ResolvedStatement resolvedStatement =
        Analyzer.analyzeStatement(sql, analyzerOptions, beamCatalog.getZetaSqlCatalog());
    ResolvedNodes.ResolvedCreateFunctionStmt createFunctionStmt =
        (ResolvedNodes.ResolvedCreateFunctionStmt) resolvedStatement;

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("ZetaSQL type TYPE_TIME not allowed in function foo");
    beamCatalog.addFunction(createFunctionStmt);
  }

  private JdbcConnection createJdbcConnection() {
    return JdbcDriver.connect(
        new ReadOnlyTableProvider("empty_table_provider", ImmutableMap.of()),
        PipelineOptionsFactory.create());
  }
}
