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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigtable;

import static org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PUSH_DOWN_OPTION;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

import com.alibaba.fastjson.JSON;
import java.util.Arrays;
import java.util.Collection;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class BigtableFilterTest {

  private static final Schema BASIC_SCHEMA =
      Schema.builder().addStringField(KEY).addStringField("name").build();

  private BeamSqlEnv sqlEnv;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"select * from TEST where key = '100'", false},
          {"select * from TEST where key >= 'key2'", false},
          {"select * from TEST where key LIKE '^key[123]'", true},
          {"select * from TEST where key LIKE '^key[abc]' OR key LIKE '^key[bcd]'", false},
        });
  }

  @Parameter public String query;

  @Parameter(1)
  public boolean isSupported;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Before
  public void buildUp() {
    TestTableProvider tableProvider = new TestTableProvider();
    Table table = getTable("TEST", PushDownOptions.NONE);
    tableProvider.createTable(table);
    tableProvider.addRows(table.getName(), row("key1", "firstName"), row("key2", "secondName"));

    sqlEnv =
        BeamSqlEnv.builder(tableProvider)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .build();
  }

  @Test
  public void testIsSupported() {
    BeamRelNode beamRelNode = sqlEnv.parseQuery(query);
    assertThat(beamRelNode, instanceOf(BeamCalcRel.class));
    BigtableFilter filter =
        new BigtableFilter(((BeamCalcRel) beamRelNode).getProgram().split().right, BASIC_SCHEMA);

    assertThat(
        "Query: '" + query + "' is expected to be " + (isSupported ? "supported." : "unsupported."),
        filter.getNotSupported().isEmpty() == isSupported);
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

  private static Row row(String key, String name) {
    return Row.withSchema(BASIC_SCHEMA).addValues(key, name).build();
  }
}
