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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import static org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PUSH_DOWN_OPTION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import com.alibaba.fastjson.JSON;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PushDownOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryFilterTest {
  // TODO: add date, time, and datetime fields.
  private static final Schema BASIC_SCHEMA =
      Schema.builder()
          .addInt32Field("unused1")
          .addInt32Field("id")
          .addStringField("name")
          .addInt16Field("unused2")
          .addBooleanField("b")
          .build();

  private BeamSqlEnv sqlEnv;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Before
  public void buildUp() {
    TestTableProvider tableProvider = new TestTableProvider();
    Table table = getTable("TEST", PushDownOptions.NONE);
    tableProvider.createTable(table);
    tableProvider.addRows(
        table.getName(),
        row(BASIC_SCHEMA, 100, 1, "one", (short) 100, true),
        row(BASIC_SCHEMA, 200, 2, "two", (short) 200, false));

    sqlEnv =
        BeamSqlEnv.builder(tableProvider)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .build();
  }

  @Test
  public void testIsSupported() {
    ImmutableList<Pair<String, Boolean>> sqlQueries =
        ImmutableList.of(
            Pair.of("select * from TEST where unused1=100", true),
            Pair.of("select * from TEST where unused1 in (100, 200)", true),
            Pair.of("select * from TEST where unused1+10=110", true),
            Pair.of("select * from TEST where b", true),
            Pair.of(
                "select * from TEST where unused1>100 and unused1<=200 and id<>1 and (name='two' or id=2)",
                true),
            Pair.of("select * from TEST where unused2=200", true),
            Pair.of("select * from TEST where name like 'o%e'", true),
            // Functions involving more than one column are not supported yet.
            Pair.of("select * from TEST where unused1=unused2 and id=2", false),
            Pair.of("select * from TEST where unused1+unused2=10", false));

    for (Pair<String, Boolean> query : sqlQueries) {
      String sql = query.getLeft();
      Boolean isSupported = query.getRight();

      BeamRelNode beamRelNode = sqlEnv.parseQuery(sql);
      assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
      BigQueryFilter filter =
          new BigQueryFilter(((BeamCalcRel) beamRelNode).getProgram().split().right);

      assertThat(
          "Query: '" + sql + "' is expected to be " + (isSupported ? "supported." : "unsupported."),
          filter.getNotSupported().isEmpty() == isSupported);
    }
  }

  private static Table getTable(String name, PushDownOptions options) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .schema(BASIC_SCHEMA)
        .properties(
            JSON.parseObject("{ " + PUSH_DOWN_OPTION + ": " + "\"" + options.toString() + "\" }"))
        .type("test")
        .build();
  }

  private static Row row(Schema schema, Object... objects) {
    return Row.withSchema(schema).addValues(objects).build();
  }
}
