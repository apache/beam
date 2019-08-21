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

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
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
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;

/** A test of {@link ReadSpannerSchemaTest}. */
@RunWith(JUnit4.class)
public class ReadSpannerSchemaTest {

  @Rule public final transient ExpectedException thrown = ExpectedException.none();

  private FakeServiceFactory serviceFactory;
  private ReadOnlyTransaction mockTx;

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
                  public boolean matches(Statement argument) {
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
                  public boolean matches(Statement argument) {
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
    mockTx = mock(ReadOnlyTransaction.class);
  }

  @Test
  public void simple() throws Exception {
    // Simplest schema: a table with int64 key
    ReadOnlyTransaction tx = mock(ReadOnlyTransaction.class);
    when(serviceFactory.mockDatabaseClient().readOnlyTransaction()).thenReturn(tx);

    preparePkMetadata(tx, Arrays.asList(pkMetadata("test", "key", "ASC")));
    prepareColumnMetadata(tx, Arrays.asList(columnMetadata("test", "key", "INT64")));

    SpannerConfig config =
        SpannerConfig.create()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withServiceFactory(serviceFactory);

    DoFnTester<Void, SpannerSchema> tester = DoFnTester.of(new ReadSpannerSchema(config));
    List<SpannerSchema> schemas = tester.processBundle(Arrays.asList((Void) null));

    assertEquals(1, schemas.size());

    SpannerSchema schema = schemas.get(0);

    assertEquals(1, schema.getTables().size());

    SpannerSchema.Column column = SpannerSchema.Column.create("key", Type.int64());
    SpannerSchema.KeyPart keyPart = SpannerSchema.KeyPart.create("key", false);

    assertThat(schema.getColumns("test"), contains(column));
    assertThat(schema.getKeyParts("test"), contains(keyPart));
  }
}
