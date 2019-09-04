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
package org.apache.beam.sdk.extensions.sql;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.Before;
import org.junit.Ignore;

/** UnitTest for Explain Plan. */
public class BeamSqlExplainTest {
  private InMemoryMetaStore metaStore;
  private BeamSqlCli cli;

  @Before
  public void setUp() throws SqlParseException, RelConversionException, ValidationException {
    metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());
    cli = new BeamSqlCli().metaStore(metaStore);

    cli.execute(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' ");

    cli.execute(
        "CREATE EXTERNAL TABLE A (\n"
            + "c1 int COMMENT 'c1',\n"
            + "c2 int COMMENT 'c2')\n"
            + "TYPE 'text'\n"
            + "COMMENT '' ");

    cli.execute(
        "CREATE EXTERNAL TABLE B (\n"
            + "c1 int COMMENT 'c1',\n"
            + "c2 int COMMENT 'c2')\n"
            + "TYPE 'text'\n"
            + "COMMENT '' ");
  }

  // TODO: (BEAM-4561) 5/30/2017 The test here is too fragile.
  @Ignore
  public void testExplainCommaJoin() {
    String plan = cli.explainQuery("SELECT A.c1, B.c2 FROM A, B WHERE A.c1 = B.c2 AND A.c1 > 0");

    assertEquals(
        "BeamCalcRel(expr#0..3=[{inputs}], c1=[$t0], c2=[$t3])\n"
            + "  BeamCoGBKJoinRel(condition=[=($0, $3)], joinType=[inner])\n"
            + "    BeamCalcRel(expr#0..1=[{inputs}], expr#2=[0], expr#3=[>($t0, $t2)],"
            + " proj#0..1=[{exprs}], $condition=[$t3])\n"
            + "      BeamIOSourceRel(table=[[beam, A]])\n"
            + "    BeamIOSourceRel(table=[[beam, B]])\n",
        plan);
  }
}
