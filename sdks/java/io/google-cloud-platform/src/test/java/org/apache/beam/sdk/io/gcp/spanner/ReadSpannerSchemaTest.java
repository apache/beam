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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatcher;

/** A test of {@link ReadSpannerSchemaTest}. */
public class ReadSpannerSchemaTest {

  @Rule public final transient ExpectedException thrown = ExpectedException.none();
  @Rule public TestPipeline p = TestPipeline.create();

  private FakeServiceFactory serviceFactory;
  private ReadOnlyTransaction mockTx;
  private SpannerConfig config;

  private static Struct columnMetadata(String tableName, String columnName, String type) {
    return Struct.newBuilder()
        .set("table_name")
        .to(tableName)
        .set("column_name")
        .to(columnName)
        .set("spanner_type")
        .to(type)
        .set("cells_mutated")
        .to(3L)
        .build();
  }

  private static Struct pkMetadata(String tableName, String columnName, String ordering) {
    return Struct.newBuilder()
        .set("table_name")
        .to(tableName)
        .set("column_name")
        .to(columnName)
        .set("column_ordering")
        .to(ordering)
        .build();
  }

  private void prepareColumnMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
    Type type =
        Type.struct(
            Type.StructField.of("table_name", Type.string()),
            Type.StructField.of("column_name", Type.string()),
            Type.StructField.of("spanner_type", Type.string()),
            Type.StructField.of("cells_mutated", Type.int64()));
    when(tx.executeQuery(
            argThat(
                new ArgumentMatcher<Statement>() {

                  @Override
                  public boolean matches(Object argument) {
                    if (!(argument instanceof Statement)) {
                      return false;
                    }
                    Statement st = (Statement) argument;
                    return st.getSql().contains("information_schema.columns");
                  }
                })))
        .thenReturn(ResultSets.forRows(type, rows));
  }

  private void preparePkMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
    Type type =
        Type.struct(
            Type.StructField.of("table_name", Type.string()),
            Type.StructField.of("column_name", Type.string()),
            Type.StructField.of("column_ordering", Type.string()));
    when(tx.executeQuery(
            argThat(
                new ArgumentMatcher<Statement>() {

                  @Override
                  public boolean matches(Object argument) {
                    if (!(argument instanceof Statement)) {
                      return false;
                    }
                    Statement st = (Statement) argument;
                    return st.getSql().contains("information_schema.index_columns");
                  }
                })))
        .thenReturn(ResultSets.forRows(type, rows));
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    serviceFactory = new FakeServiceFactory();
    // Simplest schema: a table with int64 key
    mockTx = mock(ReadOnlyTransaction.class);
    when(serviceFactory.mockDatabaseClient().readOnlyTransaction()).thenReturn(mockTx);
    preparePkMetadata(mockTx, Arrays.asList(pkMetadata("test", "key", "ASC")));
    prepareColumnMetadata(mockTx, Arrays.asList(columnMetadata("test", "key", "INT64")));
    config =
        SpannerConfig.create()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withServiceFactory(serviceFactory);
  }

  @Test
  @Category(ValidatesRunner.class)
  public void simpleColumn() {
    SchemaToColumnFunction schemaToColumnFunction = new SchemaToColumnFunction();

    PCollection<SpannerSchema.Column> columns =
        p.apply(Create.of(Arrays.asList((Void) null)))
            .apply(ParDo.of(new ReadSpannerSchema(config)))
            .apply("MapSpannerSchemaToColumn", MapElements.via(schemaToColumnFunction));

    SpannerSchema.Column column = SpannerSchema.Column.create("key", Type.int64());

    PAssert.that(columns).containsInAnyOrder(column);
    p.run().waitUntilFinish();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void simpleKeyPart() {
    SchemaToKeyPartFunction schemaToKeyPartFunction = new SchemaToKeyPartFunction();

    PCollection<SpannerSchema.KeyPart> keyParts =
        p.apply(Create.of(Arrays.asList((Void) null)))
            .apply(ParDo.of(new ReadSpannerSchema(config)))
            .apply("Map2", MapElements.via(schemaToKeyPartFunction));

    SpannerSchema.KeyPart keyPart = SpannerSchema.KeyPart.create("key", false);

    PAssert.that(keyParts).containsInAnyOrder(keyPart);
    p.run().waitUntilFinish();
  }

  //  This class to avoid serialisation of  ReadSpannerSchemaTest.class
  static class SchemaToColumnFunction extends SimpleFunction<SpannerSchema, SpannerSchema.Column> {
    @Override
    public SpannerSchema.Column apply(final SpannerSchema input) {
      return input.getColumns("test").get(0);
    }
  }

  //  This class to avoid serialisation of  ReadSpannerSchemaTest.class
  static class SchemaToKeyPartFunction
      extends SimpleFunction<SpannerSchema, SpannerSchema.KeyPart> {
    @Override
    public SpannerSchema.KeyPart apply(final SpannerSchema input) {
      return input.getKeyParts("test").get(0);
    }
  }
}
