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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.calcite.tools.ValidationException;
import org.junit.Test;

/**
 * UnitTest for {@link BeamSqlCli}.
 */
public class BeamSqlCliTest {
  @Test
  public void testExecute_createTextTable() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli()
        .metaStore(metaStore);
    cli.execute(
        "create table person (\n"
        + "id int COMMENT 'id', \n"
        + "name varchar(31) COMMENT 'name', \n"
        + "age int COMMENT 'age') \n"
        + "TYPE 'text' \n"
        + "COMMENT '' LOCATION '/home/admin/orders'"
    );
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
  }

  @Test
  public void testExecute_dropTable() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli()
        .metaStore(metaStore);
    cli.execute(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'"
    );
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);

    cli.execute("drop table person");
    table = metaStore.getTables().get("person");
    assertNull(table);
  }

  @Test(expected = ValidationException.class)
  public void testExecute_dropTable_assertTableRemovedFromPlanner() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli()
        .metaStore(metaStore);
    cli.execute(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'"
    );
    cli.execute("drop table person");
    cli.explainQuery("select * from person");
  }


  @Test
  public void testExplainQuery() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli()
        .metaStore(metaStore);

    cli.execute(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'"
    );

    String plan = cli.explainQuery("select * from person");
    assertEquals(
        "BeamProjectRel(id=[$0], name=[$1], age=[$2])\n"
        + "  BeamIOSourceRel(table=[[beam, person]])\n",
        plan
    );
  }
}
