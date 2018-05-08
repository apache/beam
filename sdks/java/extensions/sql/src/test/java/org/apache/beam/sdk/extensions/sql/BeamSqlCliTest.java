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

import static org.apache.beam.sdk.extensions.sql.RowSqlTypes.BOOLEAN;
import static org.apache.beam.sdk.extensions.sql.RowSqlTypes.INTEGER;
import static org.apache.beam.sdk.extensions.sql.RowSqlTypes.VARCHAR;
import static org.apache.beam.sdk.schemas.Schema.TypeName.ARRAY;
import static org.apache.beam.sdk.schemas.Schema.TypeName.MAP;
import static org.apache.beam.sdk.schemas.Schema.TypeName.ROW;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.schemas.Schema.Field;
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
        + "name varchar COMMENT 'name', \n"
        + "age int COMMENT 'age') \n"
        + "TYPE 'text' \n"
        + "COMMENT '' LOCATION '/home/admin/orders'"
    );
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream
            .of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true))
            .collect(toSchema()),
        table.getSchema());
  }

  @Test
  public void testExecute_createTableWithPrefixArrayField() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli()
        .metaStore(metaStore);
    cli.execute(
        "create table person (\n"
        + "id int COMMENT 'id', \n"
        + "name varchar COMMENT 'name', \n"
        + "age int COMMENT 'age', \n"
        + "tags ARRAY<VARCHAR>, \n"
        + "matrix ARRAY<ARRAY<INTEGER>> \n"
        + ") \n"
        + "TYPE 'text' \n"
        + "COMMENT '' LOCATION '/home/admin/orders'"
    );
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream
            .of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true),
                Field.of("tags",
                         ARRAY.type().withCollectionElementType(VARCHAR)).withNullable(true),
                Field.of("matrix",
                         ARRAY.type().withCollectionElementType(
                             ARRAY.type().withCollectionElementType(INTEGER))).withNullable(true))
            .collect(toSchema()),
        table.getSchema());
  }

  @Test
  public void testExecute_createTableWithPrefixMapField() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli()
        .metaStore(metaStore);
    cli.execute(
        "create table person (\n"
        + "id int COMMENT 'id', \n"
        + "name varchar COMMENT 'name', \n"
        + "age int COMMENT 'age', \n"
        + "tags MAP<VARCHAR, VARCHAR>, \n"
        + "nestedMap MAP<INTEGER, MAP<VARCHAR, INTEGER>> \n"
        + ") \n"
        + "TYPE 'text' \n"
        + "COMMENT '' LOCATION '/home/admin/orders'"
    );
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream
            .of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true),
                Field.of("tags",
                         MAP.type().withMapType(VARCHAR, VARCHAR)).withNullable(true),
                Field.of("nestedmap",
                         MAP.type().withMapType(
                             INTEGER,
                             MAP.type().withMapType(VARCHAR, INTEGER))).withNullable(true))
            .collect(toSchema()),
        table.getSchema());
  }

  @Test
  public void testExecute_createTableWithRowField() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli()
        .metaStore(metaStore);
    cli.execute(
        "create table person (\n"
        + "id int COMMENT 'id', \n"
        + "name varchar COMMENT 'name', \n"
        + "age int COMMENT 'age', \n"
        + "address ROW ( \n"
        + "  street VARCHAR, \n"
        + "  country VARCHAR \n"
        + "  ), \n"
        + "addressAngular ROW< \n"
        + "  street VARCHAR, \n"
        + "  country VARCHAR \n"
        + "  >, \n"
        + "isRobot BOOLEAN"
        + ") \n"
        + "TYPE 'text' \n"
        + "COMMENT '' LOCATION '/home/admin/orders'"
    );
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream
            .of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true),
                Field.of("address",
                         ROW.type().withRowSchema(
                             RowSqlTypes
                                 .builder()
                                 .withVarcharField("street")
                                 .withVarcharField("country")
                                 .build())).withNullable(true),
                Field.of("addressangular",
                         ROW.type().withRowSchema(
                             RowSqlTypes
                                 .builder()
                                 .withVarcharField("street")
                                 .withVarcharField("country")
                                 .build())).withNullable(true),
                Field.of("isrobot", BOOLEAN).withNullable(true))
            .collect(toSchema()),
        table.getSchema());
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
            + "name varchar COMMENT 'name', \n"
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
            + "name varchar COMMENT 'name', \n"
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
            + "name varchar COMMENT 'name', \n"
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
