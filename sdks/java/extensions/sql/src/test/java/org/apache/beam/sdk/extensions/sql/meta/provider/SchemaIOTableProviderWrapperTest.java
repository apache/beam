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
package org.apache.beam.sdk.extensions.sql.meta.provider;

import com.alibaba.fastjson.JSON;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@link org.apache.beam.sdk.extensions.sql.meta.provider.SchemaIOTableProviderWrapper} using
 * {@link org.apache.beam.sdk.extensions.sql.meta.provider.TestSchemaIOTableProviderWrapper}.
 */
@RunWith(JUnit4.class)
public class SchemaIOTableProviderWrapperTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static final Schema inputSchema =
      Schema.builder()
          .addStringField("f_string")
          .addInt64Field("f_long")
          .addBooleanField("f_bool")
          .build();
  private static final List<Row> rows =
      ImmutableList.of(
          Row.withSchema(inputSchema).addValues("zero", 0L, false).build(),
          Row.withSchema(inputSchema).addValues("one", 1L, true).build());
  private final Table testTable =
      Table.builder()
          .name("table")
          .comment("table")
          .schema(inputSchema)
          .properties(JSON.parseObject("{}"))
          .type("test")
          .build();

  @BeforeClass
  public static void setUp() {
    TestSchemaIOTableProviderWrapper.addRows(rows.stream().toArray(Row[]::new));
  }

  @Test
  public void testBuildIOReader() {
    TestSchemaIOTableProviderWrapper provider = new TestSchemaIOTableProviderWrapper();
    BeamSqlTable beamSqlTable = provider.buildBeamSqlTable(testTable);

    PCollection<Row> result = beamSqlTable.buildIOReader(pipeline.begin());
    PAssert.that(result).containsInAnyOrder(rows);

    pipeline.run();
  }

  @Test
  public void testBuildIOReader_withProjectionPushdown() {
    TestSchemaIOTableProviderWrapper provider = new TestSchemaIOTableProviderWrapper();
    BeamSqlTable beamSqlTable = provider.buildBeamSqlTable(testTable);

    PCollection<Row> result =
        beamSqlTable.buildIOReader(
            pipeline.begin(),
            new DefaultTableFilter(ImmutableList.of()),
            ImmutableList.of("f_long"));
    Schema outputSchema = Schema.builder().addInt64Field("f_long").build();
    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(outputSchema).addValues(0L).build(),
            Row.withSchema(outputSchema).addValues(1L).build());

    pipeline.run();
  }
}
