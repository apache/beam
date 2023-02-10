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
package org.apache.beam.sdk.io.singlestore;

import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SingleStoreIODefaultMapperIT {
  private static final String DATABASE_NAME = "SingleStoreIOIT";

  private static int numberOfRows;

  private static String tableName;

  private static String serverName;

  private static String username;

  private static String password;

  private static Integer port;

  private static SingleStoreIO.DataSourceConfiguration dataSourceConfiguration;

  private static Schema schema;

  @BeforeClass
  public static void setup() {
    SingleStoreIOTestPipelineOptions options;
    try {
      options = readIOTestPipelineOptions(SingleStoreIOTestPipelineOptions.class);
    } catch (IllegalArgumentException e) {
      options = null;
    }
    org.junit.Assume.assumeNotNull(options);

    numberOfRows = options.getNumberOfRecords();
    serverName = options.getSingleStoreServerName();
    username = options.getSingleStoreUsername();
    password = options.getSingleStorePassword();
    port = options.getSingleStorePort();
    tableName = DatabaseTestHelper.getTestTableName("IT");
    dataSourceConfiguration =
        SingleStoreIO.DataSourceConfiguration.create(serverName + ":" + port)
            .withDatabase(DATABASE_NAME)
            .withPassword(password)
            .withUsername(username);

    generateSchema();
  }

  private static void generateSchema() {
    Schema.Builder schemaBuilder = new Schema.Builder();
    schemaBuilder.addField("c1", Schema.FieldType.BOOLEAN);
    schemaBuilder.addField("c2", Schema.FieldType.BYTE);
    schemaBuilder.addField("c3", Schema.FieldType.INT16);
    schemaBuilder.addField("c4", Schema.FieldType.INT32);
    schemaBuilder.addField("c5", Schema.FieldType.INT64);
    schemaBuilder.addField("c6", Schema.FieldType.FLOAT);
    schemaBuilder.addField("c7", Schema.FieldType.DOUBLE);
    schemaBuilder.addField("c8", Schema.FieldType.DECIMAL);
    schemaBuilder.addField("c9", Schema.FieldType.DATETIME);
    schemaBuilder.addField("c10", Schema.FieldType.DATETIME);
    schemaBuilder.addField("c11", Schema.FieldType.DATETIME);
    schemaBuilder.addField("c12", Schema.FieldType.BYTES);
    schemaBuilder.addField("c13", Schema.FieldType.BYTES);
    schemaBuilder.addField("c14", Schema.FieldType.BYTES);
    schemaBuilder.addField("c15", Schema.FieldType.STRING);
    schemaBuilder.addField("c16", Schema.FieldType.STRING);
    schemaBuilder.addField("c17", Schema.FieldType.STRING);

    schema = schemaBuilder.build();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteThenRead() throws Exception {
    TestHelper.createDatabaseIfNotExists(serverName, port, username, password, DATABASE_NAME);
    createTable();

    try {
      PipelineResult writeResult = runWrite();
      assertEquals(PipelineResult.State.DONE, writeResult.waitUntilFinish());
      PipelineResult readResult = runRead();
      assertEquals(PipelineResult.State.DONE, readResult.waitUntilFinish());
    } finally {
      DataSource dataSource = dataSourceConfiguration.getDataSource();
      DatabaseTestHelper.deleteTable(dataSource, tableName);
    }
  }

  private void createTable() throws SQLException {
    DataSource dataSource = dataSourceConfiguration.getDataSource();
    Connection conn = dataSource.getConnection();
    try {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
      stmt.executeUpdate(
          "CREATE TABLE "
              + tableName
              + "("
              + "c1 BIT, "
              + "c2 TINYINT, "
              + "c3 SMALLINT, "
              + "c4 INTEGER, "
              + "c5 BIGINT, "
              + "c6 FLOAT, "
              + "c7 DOUBLE, "
              + "c8 DECIMAL(10, 5), "
              + "c9 TIMESTAMP, "
              + "c10 DATE, "
              + "c11 TIME, "
              + "c12 BLOB, "
              + "c13 TINYBLOB, "
              + "c14 BINARY(10), "
              + "c15 MEDIUMTEXT, "
              + "c16 TINYTEXT, "
              + "c17 CHAR(10) "
              + ")");
    } finally {
      conn.close();
    }
  }

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  public static class ConstructRowFn extends DoFn<Long, Row> {
    @ProcessElement
    public void processElement(ProcessContext c) {

      Row.Builder rowBuilder = Row.withSchema(schema);
      rowBuilder.addValue(Boolean.TRUE);
      rowBuilder.addValue((byte) 10);
      rowBuilder.addValue((short) 10);
      rowBuilder.addValue(10);
      rowBuilder.addValue((long) 10);
      rowBuilder.addValue((float) 10.1);
      rowBuilder.addValue(10.1);
      rowBuilder.addValue(new BigDecimal("10.1"));
      rowBuilder.addValue(new DateTime("2022-01-01T10:10:10Z"));
      rowBuilder.addValue(new DateTime("2022-01-01T10:10:10Z"));
      rowBuilder.addValue(new DateTime("2022-01-01T10:10:10Z"));
      rowBuilder.addValue("asd".getBytes(StandardCharsets.UTF_8));
      rowBuilder.addValue("asd".getBytes(StandardCharsets.UTF_8));
      rowBuilder.addValue("asd".getBytes(StandardCharsets.UTF_8));
      rowBuilder.addValue("asd");
      rowBuilder.addValue("asd");
      rowBuilder.addValue("asd");
      Row row = rowBuilder.build();

      c.output(row);
    }
  }

  private PipelineResult runWrite() {
    pipelineWrite
        .apply(GenerateSequence.from(0).to(numberOfRows))
        .apply(ParDo.of(new ConstructRowFn()))
        .setRowSchema(schema)
        .apply(
            SingleStoreIO.writeRows()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withTable(tableName));

    return pipelineWrite.run();
  }

  private PipelineResult runRead() {
    PCollection<Row> res =
        pipelineRead.apply(
            SingleStoreIO.readRows()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withTable(tableName));

    PAssert.thatSingleton(res.apply("Count All", Count.globally())).isEqualTo((long) numberOfRows);

    res.apply(ParDo.of(new CheckDoFn())).setCoder(VoidCoder.of());

    return pipelineRead.run();
  }

  private static class CheckDoFn extends DoFn<Row, Void> {
    @ProcessElement
    public void process(ProcessContext c) {
      Row r = c.element();
      Assert.assertNotNull(r);
      assertEquals(Boolean.TRUE, r.getBoolean(0));
      assertEquals((Byte) (byte) 10, r.getByte(1));
      assertEquals((Short) (short) 10, r.getInt16(2));
      assertEquals((Integer) 10, r.getInt32(3));
      assertEquals((Long) (long) 10, r.getInt64(4));
      assertEquals((Float) (float) 10.1, r.getFloat(5));
      assertEquals((Double) 10.1, r.getDouble(6));
      assertEquals(new BigDecimal("10.10000"), r.getDecimal(7));
      assertEquals(0, new DateTime("2022-01-01T10:10:10Z").compareTo(r.getDateTime(8)));
      assertEquals(0, new DateTime("2022-01-01T00:00:00Z").compareTo(r.getDateTime(9)));
      assertEquals(0, new DateTime("1970-01-01T10:10:10Z").compareTo(r.getDateTime(10)));
      assertArrayEquals("asd".getBytes(StandardCharsets.UTF_8), r.getBytes(11));
      assertArrayEquals("asd".getBytes(StandardCharsets.UTF_8), r.getBytes(12));
      assertArrayEquals("asd\0\0\0\0\0\0\0".getBytes(StandardCharsets.UTF_8), r.getBytes(13));
      assertEquals("asd", r.getString(14));
      assertEquals("asd", r.getString(15));
      assertEquals("asd", r.getString(16));
    }
  }
}
