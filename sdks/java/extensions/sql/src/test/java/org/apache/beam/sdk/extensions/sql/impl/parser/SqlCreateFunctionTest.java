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

import static org.junit.Assert.fail;

import org.apache.beam.sdk.extensions.sql.BeamSqlDslBase;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SqlCreateFunction}. */
@RunWith(JUnit4.class)
public class SqlCreateFunctionTest extends BeamSqlDslBase {
  private final String jarPathProperty = "beam.sql.udf.test.jar_path";
  private final @Nullable String jarPath = System.getProperty(jarPathProperty);

  @Before
  public void setUp() {
    if (jarPath == null) {
      fail(
          String.format(
              "System property %s must be set to run %s.",
              jarPathProperty, SqlCreateFunctionTest.class.getSimpleName()));
    }
  }

  @Test
  public void createScalarFunction() throws Exception {
    String ddl = String.format("CREATE FUNCTION increment USING JAR '%s'", jarPath);
    String query = "SELECT increment(0)";
    PCollection<Row> stream = pipeline.apply(SqlTransform.query(query).withDdlString(ddl));

    final Schema schema = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(1L).build());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void createAggregateFunction() throws Exception {
    String ddl = String.format("CREATE AGGREGATE FUNCTION my_sum USING JAR '%s'", jarPath);
    String query = "SELECT my_sum(f_long) FROM PCOLLECTION";
    PCollection<Row> stream = boundedInput1.apply(SqlTransform.query(query).withDdlString(ddl));

    final Schema schema = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(10000L).build());

    pipeline.run().waitUntilFinish();
  }
}
