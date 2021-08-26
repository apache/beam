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

import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.checkMessage;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InvalidTableException;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.junit.Before;
import org.junit.Test;

public class BigtableTableCreationFailuresTest {

  private final InMemoryMetaStore metaStore = new InMemoryMetaStore();
  private final TableProvider tableProvider = new BigtableTableProvider();
  private BeamSqlCli cli;

  @Before
  public void setUp() {
    metaStore.registerProvider(tableProvider);
    cli = new BeamSqlCli().metaStore(metaStore);
  }

  @Test
  public void testCreateWithoutTypeFails() {
    String createTable = "CREATE EXTERNAL TABLE failure(something VARCHAR)";
    ParseException e = assertThrows(ParseException.class, () -> cli.execute(createTable));
    checkMessage(e.getMessage(), "Unable to parse query");
  }

  @Test
  public void testCreateWithoutLocationFails() {
    String createTable =
        "CREATE EXTERNAL TABLE fail(key VARCHAR, something VARCHAR) \n" + "TYPE bigtable \n";
    cli.execute(createTable);
    Table table = metaStore.getTables().get("fail");

    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> tableProvider.buildBeamSqlTable(table));
    checkMessage(e.getMessage(), "LOCATION");
  }

  @Test
  public void testCreateWithoutKeyFails() {
    String createTable =
        "CREATE EXTERNAL TABLE fail(something VARCHAR) \n"
            + "TYPE bigtable \n"
            + "LOCATION '"
            + location()
            + "'";
    cli.execute(createTable);
    Table table = metaStore.getTables().get("fail");
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> tableProvider.buildBeamSqlTable(table));
    checkMessage(e.getMessage(), "Schema has to contain 'key' field");
  }

  @Test
  public void testCreateWrongKeyTypeFails() {
    String createTable =
        "CREATE EXTERNAL TABLE fail(key FLOAT) \n"
            + "TYPE bigtable \n"
            + "LOCATION '"
            + location()
            + "'";
    cli.execute(createTable);
    Table table = metaStore.getTables().get("fail");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> tableProvider.buildBeamSqlTable(table));
    checkMessage(e.getMessage(), "key field type should be STRING but was FLOAT");
  }

  @Test
  public void testCreatePropertiesDontMatchSchema() {
    String createTable =
        "CREATE EXTERNAL TABLE fail(key VARCHAR, q BIGINT, qq BINARY) \n"
            + "TYPE bigtable \n"
            + "LOCATION '"
            + location()
            + "' \n"
            + "TBLPROPERTIES '{\"columnsMapping\": \"f:b,f:c\"}'";
    cli.execute(createTable);
    Table table = metaStore.getTables().get("fail");
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> tableProvider.buildBeamSqlTable(table));
    checkMessage(e.getMessage(), "does not fit to schema field names");
  }

  @Test
  public void testCreatePropertiesCountNotEqualSchemaFields() {
    String createTable =
        "CREATE EXTERNAL TABLE fail(key VARCHAR, q BIGINT, qq BINARY) \n"
            + "TYPE bigtable \n"
            + "LOCATION '"
            + location()
            + "' \n"
            + "TBLPROPERTIES '{\"columnsMapping\": \"f:q\"}'";
    cli.execute(createTable);
    Table table = metaStore.getTables().get("fail");
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> tableProvider.buildBeamSqlTable(table));
    checkMessage(e.getMessage(), "Schema fields count: '2' does not fit columnsMapping count: '1'");
  }

  @Test
  public void testShouldFailOnIncorrectLocation() {
    String createTable =
        "CREATE EXTERNAL TABLE fail(key VARCHAR, q BIGINT) \n"
            + "TYPE bigtable \n"
            + "LOCATION 'googleapis.com/incorrect/projects/fakeProject/instances/fakeInstance/tables/beamTable' \n"
            + "TBLPROPERTIES '{\"columnsMapping\": \"f:q\"}'";
    cli.execute(createTable);
    Table table = metaStore.getTables().get("fail");
    InvalidTableException e =
        assertThrows(InvalidTableException.class, () -> tableProvider.buildBeamSqlTable(table));
    checkMessage(e.getMessage(), "Bigtable location must be in the following format:");
  }

  private static String location() {
    return "googleapis.com/bigtable/projects/fakeProject/instances/fakeInstance/tables/beamTable";
  }
}
