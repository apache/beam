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
package org.apache.beam.dsls.sql.integrationtest;

import java.sql.Types;
import org.apache.beam.dsls.sql.BeamSqlCli;
import org.apache.beam.dsls.sql.BeamSqlEnv;
import org.apache.beam.dsls.sql.TestUtils;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration test for string functions.
 */
public class BeamSqlStringFunctionsIntegrationTest {
  static BeamSqlEnv sqlEnv = new BeamSqlEnv();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testStringFunctions() throws Exception {
    String sql = "SELECT "
        + "'hello' || ' world' as concat,"
        + "CHAR_LENGTH('hello') as cl,"
        + "CHARACTER_LENGTH('hello') as cl1,"
        + "UPPER('hello') as up,"
        + "LOWER('HELLO') as lo,"
        + "POSITION('world' IN 'helloworld') as po,"
        + "POSITION('world' IN 'helloworld' FROM 1) as po1,"
        + "TRIM(' hello ') as tr,"
        + "TRIM(LEADING ' ' FROM ' hello ') as tr1,"
        + "TRIM(TRAILING ' ' FROM ' hello ') as tr2,"
        + "TRIM(BOTH ' ' FROM ' hello ') as tr3,"
        + "OVERLAY('w3333333rce' PLACING 'resou' FROM 3) as ol,"
        + "SUBSTRING('hello' FROM 2) as ss,"
        + "SUBSTRING('hello' FROM 2 FOR 2) as ss1,"
        + "INITCAP('hello world') as ss1"
    ;

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            // 1 -> 5
            Types.VARCHAR, "concat",
            Types.INTEGER, "cl",
            Types.INTEGER, "cl1",
            Types.VARCHAR, "up",
            Types.VARCHAR, "lo",
            // 6 -> 10
            Types.INTEGER, "po",
            Types.INTEGER, "po1",
            Types.VARCHAR, "tr",
            Types.VARCHAR, "tr1",
            Types.VARCHAR, "tr2",
            // 11 -> 15
            Types.VARCHAR, "tr3",
            Types.VARCHAR, "ol",
            Types.VARCHAR, "ss",
            Types.VARCHAR, "ss1",
            Types.VARCHAR, "ic"
        ).addRows(
            // 1 -> 5(lo)
            "hello world", 5, 5, "HELLO", "hello",
            // 6 -> 10()
            5, 5, "hello", "hello ", " hello",
            // 11 -> 15
            "hello", "w3resou3rce", "ello", "el", "Hello World"
        ).getRows());
    pipeline.run();
  }

}
