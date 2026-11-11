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
package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.extensions.sql.jdbc.BeamSqlLine;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * {@code CHANGES(...)} and {@code INSERT INTO ... WITH} on Iceberg tables from the Beam SQL shell.
 */
public class BeamSqlLineIcebergCdcTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Test
  public void shellAppliesChangesReadsThemAndMirrorsThem() throws Exception {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    assertTrue(warehouseFile.delete());
    String createCatalog =
        "CREATE CATALOG cdc_catalog TYPE iceberg PROPERTIES ('type' = 'hadoop', 'warehouse' = 'file:"
            + warehouseFile
            + "/"
            + UUID.randomUUID()
            + "');";
    String applyChanges =
        "INSERT INTO orders WITH ('mode' = 'merge-on-read', 'change_type_column' = 'op',"
            + " 'sequence_number_column' = 'seq') VALUES ";

    List<List<String>> output =
        runShell(
            createCatalog,
            "USE CATALOG cdc_catalog;",
            "CREATE DATABASE db;",
            "USE DATABASE db;",
            "CREATE EXTERNAL TABLE orders (id BIGINT, name VARCHAR,"
                + " PRIMARY KEY (id) NOT ENFORCED) TYPE 'iceberg';",
            applyChanges + "(1, 'a', 'INSERT', 1), (2, 'x', 'INSERT', 1);",
            applyChanges
                + "(1, 'a', 'UPDATE_BEFORE', 2), (1, 'b', 'UPDATE_AFTER', 2), (2, 'x', 'DELETE', 2);",
            "SELECT * FROM CHANGES(orders) WITH ('include_metadata_columns' ="
                + " ARRAY['_change_type', '_commit_snapshot_sequence_number']);",
            "CREATE EXTERNAL TABLE orders_mirror (id BIGINT, name VARCHAR,"
                + " PRIMARY KEY (id) NOT ENFORCED) TYPE 'iceberg';",
            "INSERT INTO orders_mirror WITH ('mode' = 'merge-on-read') SELECT * FROM"
                + " CHANGES(orders) WITH ('include_metadata_columns' ="
                + " ARRAY['_commit_snapshot_sequence_number']);");

    assertThat(
        output,
        hasItems(
            ImmutableList.of("id", "name", "_change_type", "_commit_snapshot_sequence_number"),
            ImmutableList.of("1", "a", "INSERT", "1"),
            ImmutableList.of("2", "x", "INSERT", "1"),
            ImmutableList.of("1", "a", "UPDATE_BEFORE", "2"),
            ImmutableList.of("1", "b", "UPDATE_AFTER", "2"),
            ImmutableList.of("2", "x", "DELETE", "2")));

    // A second session loads both tables from the catalog, primary keys included.
    List<List<String>> mirror =
        runShell(
            createCatalog,
            "USE CATALOG cdc_catalog;",
            "USE DATABASE db;",
            "SELECT * FROM orders_mirror;",
            "SELECT id, _change_type FROM CHANGES(orders)"
                + " WITH ('include_metadata_columns' = ARRAY['_change_type']) WHERE id = 2;");

    assertThat(
        mirror,
        hasItems(
            ImmutableList.of("id", "name"),
            ImmutableList.of("1", "b"),
            ImmutableList.of("id", "_change_type"),
            ImmutableList.of("2", "INSERT"),
            ImmutableList.of("2", "DELETE")));
    assertThat(mirror, not(hasItems(ImmutableList.of("2", "x"))));
  }

  /** Runs the statements through the shell's entry point and returns its printed rows. */
  private static List<List<String>> runShell(String... statements) throws Exception {
    List<String> args = new ArrayList<>();
    for (String statement : statements) {
      args.add("-e");
      args.add(statement);
    }
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8.name()));
    try {
      BeamSqlLine.main(args.toArray(new String[0]));
    } finally {
      System.setOut(originalOut);
    }
    return Arrays.stream(captured.toString(StandardCharsets.UTF_8.name()).split("\n"))
        .map(
            line ->
                Arrays.stream(line.split("\\|"))
                    .map(String::trim)
                    .filter(field -> !field.isEmpty())
                    .collect(toList()))
        .collect(toList());
  }
}
