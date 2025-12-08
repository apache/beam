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
package org.apache.beam.sdk.extensions.sql.jdbc;

import static org.apache.beam.sdk.extensions.sql.jdbc.BeamSqlLineTestingUtils.buildArgs;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class BeamSqlLineShowTests {
  @Test
  public void testShowTables() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    String[] args =
        buildArgs(
            "CREATE DATABASE other_db",
            "CREATE EXTERNAL TABLE other_db.should_not_show_up (id int, name varchar) TYPE 'text'",
            "CREATE CATALOG my_catalog TYPE 'local'",
            "CREATE DATABASE my_catalog.my_db",
            "USE DATABASE my_catalog.my_db",
            "CREATE EXTERNAL TABLE my_table (id int, name varchar) TYPE 'text'",
            "CREATE EXTERNAL TABLE my_other_table (col1 int, col2 timestamp) TYPE 'text'",
            "CREATE EXTERNAL TABLE my_other_table_with_a_long_name (foo varchar, bar boolean) TYPE 'test'",
            "SHOW TABLES");

    BeamSqlLine.runSqlLine(args, null, byteArrayOutputStream, null);

    List<String> lines = Arrays.asList(byteArrayOutputStream.toString("UTF-8").split("\n"));
    assertThat(
        Arrays.asList(
            "+------+------+",
            "| NAME | TYPE |",
            "+------+------+",
            "| my_other_table | text |",
            "| my_other_table_with_a_long_name | test |",
            "| my_table | text |",
            "+------+------+"),
        everyItem(is(oneOf(lines.toArray()))));
  }

  @Test
  public void testShowTablesInOtherDatabase() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    String[] args =
        buildArgs(
            "CREATE DATABASE my_db",
            "USE DATABASE my_db",
            "CREATE EXTERNAL TABLE should_not_show_up (id int, name varchar) TYPE 'text'",
            "CREATE CATALOG other_catalog TYPE 'local'",
            "CREATE DATABASE other_catalog.other_db",
            "CREATE EXTERNAL TABLE other_catalog.other_db.other_table (id int, name varchar) TYPE 'text'",
            "SHOW TABLES IN other_catalog.other_db");

    BeamSqlLine.runSqlLine(args, null, byteArrayOutputStream, null);

    List<String> lines = Arrays.asList(byteArrayOutputStream.toString("UTF-8").split("\n"));
    assertThat(
        Arrays.asList(
            "+------+------+",
            "| NAME | TYPE |",
            "+------+------+",
            "| other_table | text |",
            "+------+------+"),
        everyItem(is(oneOf(lines.toArray()))));
  }

  @Test
  public void testShowTablesWithPattern() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    String[] args =
        buildArgs(
            "CREATE DATABASE my_db",
            "USE DATABASE my_db",
            "CREATE EXTERNAL TABLE my_table (id int, name varchar) TYPE 'text'",
            "CREATE EXTERNAL TABLE my_table_2 (id int, name varchar) TYPE 'text'",
            "CREATE EXTERNAL TABLE my_foo_table_1 (id int, name varchar) TYPE 'text'",
            "CREATE EXTERNAL TABLE my_foo_table_2 (id int, name varchar) TYPE 'text'",
            "SHOW TABLES LIKE '%foo%'");

    BeamSqlLine.runSqlLine(args, null, byteArrayOutputStream, null);

    List<String> lines = Arrays.asList(byteArrayOutputStream.toString("UTF-8").split("\n"));
    assertThat(
        Arrays.asList(
            "+------+------+",
            "| NAME | TYPE |",
            "+------+------+",
            "| my_foo_table_1 | text |",
            "| my_foo_table_2 | text |",
            "+------+------+"),
        everyItem(is(oneOf(lines.toArray()))));
  }

  @Test
  public void testShowCurrentDatabase() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    String[] args =
        buildArgs(
            "CREATE DATABASE should_not_show_up",
            "CREATE CATALOG my_catalog TYPE 'local'",
            "USE CATALOG my_catalog",
            "CREATE DATABASE my_db",
            "CREATE DATABASE my_other_db",
            "CREATE DATABASE my_database_that_has_a_very_long_name",
            "USE DATABASE my_other_db",
            "SHOW CURRENT database");

    BeamSqlLine.runSqlLine(args, null, byteArrayOutputStream, null);

    List<String> lines = Arrays.asList(byteArrayOutputStream.toString("UTF-8").split("\n"));
    assertThat(
        Arrays.asList("+------+", "| NAME |", "+------+", "| my_other_db |", "+------+"),
        everyItem(is(oneOf(lines.toArray()))));
  }

  @Test
  public void testShowCurrentDatabaseWithNoneSet() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    String[] args =
        buildArgs(
            "CREATE DATABASE should_not_show_up",
            "CREATE CATALOG my_catalog TYPE 'local'",
            "USE CATALOG my_catalog",
            "DROP DATABASE `default`",
            "SHOW CURRENT DATABASE");

    BeamSqlLine.runSqlLine(args, null, byteArrayOutputStream, null);

    List<String> lines = Arrays.asList(byteArrayOutputStream.toString("UTF-8").split("\n"));
    assertThat(
        Arrays.asList("+------+", "| NAME |", "+------+", "+------+"),
        everyItem(is(oneOf(lines.toArray()))));
  }

  @Test
  public void testShowDatabases() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    String[] args =
        buildArgs(
            "CREATE DATABASE should_not_show_up",
            "CREATE CATALOG my_catalog TYPE 'local'",
            "USE CATALOG my_catalog",
            "CREATE DATABASE my_db",
            "CREATE DATABASE my_other_db",
            "CREATE DATABASE my_database_that_has_a_very_long_name",
            "SHOW DATABASES");

    BeamSqlLine.runSqlLine(args, null, byteArrayOutputStream, null);

    List<String> lines = Arrays.asList(byteArrayOutputStream.toString("UTF-8").split("\n"));
    assertThat(
        Arrays.asList(
            "+------+",
            "| NAME |",
            "+------+",
            "| default |",
            "| my_database_that_has_a_very_long_name |",
            "| my_db |",
            "| my_other_db |",
            "+------+"),
        everyItem(is(oneOf(lines.toArray()))));
  }

  @Test
  public void testShowDatabasesInOtherCatalog() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    String[] args =
        buildArgs(
            "CREATE DATABASE should_not_show_up",
            "CREATE CATALOG my_catalog TYPE 'local'",
            "USE CATALOG my_catalog",
            "CREATE DATABASE my_db",
            "CREATE CATALOG my_other_catalog TYPE 'local'",
            "CREATE DATABASE my_other_catalog.other_db",
            "SHOW DATABASES FROM my_other_catalog");

    BeamSqlLine.runSqlLine(args, null, byteArrayOutputStream, null);

    List<String> lines = Arrays.asList(byteArrayOutputStream.toString("UTF-8").split("\n"));
    assertThat(
        Arrays.asList(
            "+------+", "| NAME |", "+------+", "| default |", "| other_db |", "+------+"),
        everyItem(is(oneOf(lines.toArray()))));
  }

  @Test
  public void testShowDatabasesWithPattern() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    String[] args =
        buildArgs(
            "CREATE CATALOG my_catalog TYPE 'local'",
            "CREATE DATABASE my_catalog.my_db",
            "CREATE DATABASE my_catalog.other_db",
            "CREATE DATABASE my_catalog.some_foo_db",
            "CREATE DATABASE my_catalog.some_other_foo_db",
            "SHOW DATABASES FROM my_catalog LIKE '%foo%'");

    BeamSqlLine.runSqlLine(args, null, byteArrayOutputStream, null);

    List<String> lines = Arrays.asList(byteArrayOutputStream.toString("UTF-8").split("\n"));
    assertThat(
        Arrays.asList(
            "+------+",
            "| NAME |",
            "+------+",
            "| some_foo_db |",
            "| some_other_foo_db |",
            "+------+"),
        everyItem(is(oneOf(lines.toArray()))));
  }

  @Test
  public void testShowCurrentCatalog() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    String[] args =
        buildArgs(
            "CREATE CATALOG my_catalog TYPE 'local'",
            "CREATE CATALOG my_very_long_catalog_name TYPE 'local'",
            "SHOW CURRENT CATALOG");

    BeamSqlLine.runSqlLine(args, null, byteArrayOutputStream, null);

    List<String> lines = Arrays.asList(byteArrayOutputStream.toString("UTF-8").split("\n"));
    assertThat(
        Arrays.asList(
            "+------+------+",
            "| NAME | TYPE |",
            "+------+------+",
            "| default | local |",
            "+------+------+"),
        everyItem(is(oneOf(lines.toArray()))));
  }

  @Test
  public void testShowCatalogs() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    String[] args =
        buildArgs(
            "CREATE CATALOG my_catalog TYPE 'local'",
            "CREATE CATALOG my_very_long_catalog_name TYPE 'local'",
            "SHOW CATALOGS");

    BeamSqlLine.runSqlLine(args, null, byteArrayOutputStream, null);

    List<String> lines = Arrays.asList(byteArrayOutputStream.toString("UTF-8").split("\n"));
    assertThat(
        Arrays.asList(
            "+------+------+",
            "| NAME | TYPE |",
            "+------+------+",
            "| default | local |",
            "| my_catalog | local |",
            "| my_very_long_catalog_name | local |",
            "+------+------+"),
        everyItem(is(oneOf(lines.toArray()))));
  }

  @Test
  public void testShowCatalogsWithPattern() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    String[] args =
        buildArgs(
            "CREATE CATALOG my_catalog TYPE 'local'",
            "CREATE CATALOG my_catalog_2 TYPE 'local'",
            "CREATE CATALOG my_very_long_catalog_name TYPE 'local'",
            "SHOW CATALOGS LIKE 'my_catalog%'");

    BeamSqlLine.runSqlLine(args, null, byteArrayOutputStream, null);

    List<String> lines = Arrays.asList(byteArrayOutputStream.toString("UTF-8").split("\n"));
    assertThat(
        Arrays.asList(
            "+------+------+",
            "| NAME | TYPE |",
            "+------+------+",
            "| my_catalog | local |",
            "| my_catalog_2 | local |",
            "+------+------+"),
        everyItem(is(oneOf(lines.toArray()))));
  }
}
