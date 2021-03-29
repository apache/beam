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

import java.lang.reflect.Method;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.ScalarFunctionImpl;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.UserFunctionDefinitions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BeamZetaSqlCatalogTest {
  public static class IncrementFn implements BeamSqlUdf {
    public Long eval(Long i) {
      return i + 1;
    }
  }

  @Test
  public void loadsUserDefinedFunctionsFromSchema() throws NoSuchMethodException {
    JdbcConnection jdbcConnection =
        JdbcDriver.connect(
            new ReadOnlyTableProvider("empty_table_provider", ImmutableMap.of()),
            PipelineOptionsFactory.create());
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
}
