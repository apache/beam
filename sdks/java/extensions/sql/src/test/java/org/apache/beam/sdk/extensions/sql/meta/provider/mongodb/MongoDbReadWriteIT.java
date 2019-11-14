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
package org.apache.beam.sdk.extensions.sql.meta.provider.mongodb;

import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BYTE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT16;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.apache.beam.sdk.testing.SerializableMatchers.containsInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;

import com.mongodb.MongoClient;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongoCmdOptionsBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import java.util.Arrays;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamPushDownIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test of {@link org.apache.beam.sdk.extensions.sql.meta.provider.mongodb.MongoDbTable} on an
 * independent Mongo instance.
 */
@RunWith(JUnit4.class)
public class MongoDbReadWriteIT {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDbReadWriteIT.class);
  private static final Schema SOURCE_SCHEMA =
      Schema.builder()
          .addNullableField("_id", STRING)
          .addNullableField("c_bigint", INT64)
          .addNullableField("c_tinyint", BYTE)
          .addNullableField("c_smallint", INT16)
          .addNullableField("c_integer", INT32)
          .addNullableField("c_float", FLOAT)
          .addNullableField("c_double", DOUBLE)
          .addNullableField("c_boolean", BOOLEAN)
          .addNullableField("c_varchar", STRING)
          .addNullableField("c_arr", FieldType.array(STRING))
          .build();
  private static final String hostname = "localhost";
  private static final String database = "beam";
  private static final String collection = "collection";
  private static int port;

  @ClassRule public static final TemporaryFolder MONGODB_LOCATION = new TemporaryFolder();

  private static final MongodStarter mongodStarter = MongodStarter.getDefaultInstance();
  private static MongodExecutable mongodExecutable;
  private static MongodProcess mongodProcess;
  private static MongoClient client;

  private static final BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new MongoDbTableProvider());
  private static String mongoSqlUrl;

  @Rule public final TestPipeline writePipeline = TestPipeline.create();
  @Rule public final TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setUp() throws Exception {
    port = NetworkTestHelper.getAvailableLocalPort();
    LOG.info("Starting MongoDB embedded instance on {}", port);
    IMongodConfig mongodConfig =
        new MongodConfigBuilder()
            .version(Version.Main.PRODUCTION)
            .configServer(false)
            .replication(new Storage(MONGODB_LOCATION.getRoot().getPath(), null, 0))
            .net(new Net(hostname, port, Network.localhostIsIPv6()))
            .cmdOptions(
                new MongoCmdOptionsBuilder()
                    .syncDelay(10)
                    .useNoPrealloc(true)
                    .useSmallFiles(true)
                    .useNoJournal(true)
                    .verbose(false)
                    .build())
            .build();
    mongodExecutable = mongodStarter.prepare(mongodConfig);
    mongodProcess = mongodExecutable.start();
    client = new MongoClient(hostname, port);

    mongoSqlUrl =
        String.format(
            "mongodb://%s:%d/%s/%s",
            hostname,
            port,
            database,
            collection);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    client.dropDatabase(database);
    client.close();
    mongodProcess.stop();
    mongodExecutable.stop();
  }

  @Test
  public void testWriteAndRead() {
    Row testRow =
        row(
            SOURCE_SCHEMA,
            "object_id",
            9223372036854775807L,
            (byte) 127,
            (short) 32767,
            2147483647,
            (float) 1.0,
            1.0,
            true,
            "varchar",
            Arrays.asList("123", "456"));

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   _id VARCHAR, \n "
            + "   c_bigint BIGINT, \n "
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'mongodb' \n"
            + "LOCATION '"
            + mongoSqlUrl
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "'object_id', "
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "'varchar', "
            + "ARRAY['123', '456']"
            + ")";

    BeamRelNode insertRelNode = sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(writePipeline, insertRelNode);
    writePipeline.run().waitUntilFinish();

    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery("select * from TEST"));

    assertEquals(SOURCE_SCHEMA, output.getSchema());

    PAssert.that(output).containsInAnyOrder(testRow);

    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testProjectPushDown() {
    final Schema expectedSchema =
        Schema.builder()
            .addNullableField("c_varchar", STRING)
            .addNullableField("c_boolean", BOOLEAN)
            .addNullableField("c_integer", INT32)
            .build();
    Row testRow = row(expectedSchema, "varchar", true, 2147483647);

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   _id VARCHAR, \n "
            + "   c_bigint BIGINT, \n "
            + "   c_tinyint TINYINT, \n"
            + "   c_smallint SMALLINT, \n"
            + "   c_integer INTEGER, \n"
            + "   c_float FLOAT, \n"
            + "   c_double DOUBLE, \n"
            + "   c_boolean BOOLEAN, \n"
            + "   c_varchar VARCHAR, \n "
            + "   c_arr ARRAY<VARCHAR> \n"
            + ") \n"
            + "TYPE 'mongodb' \n"
            + "LOCATION '"
            + mongoSqlUrl
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement =
        "INSERT INTO TEST VALUES ("
            + "'object_id', "
            + "9223372036854775807, "
            + "127, "
            + "32767, "
            + "2147483647, "
            + "1.0, "
            + "1.0, "
            + "TRUE, "
            + "'varchar', "
            + "ARRAY['123', '456']"
            + ")";

    BeamRelNode insertRelNode = sqlEnv.parseQuery(insertStatement);
    BeamSqlRelUtils.toPCollection(writePipeline, insertRelNode);
    writePipeline.run().waitUntilFinish();

    BeamRelNode node = sqlEnv.parseQuery("select c_varchar, c_boolean, c_integer from TEST");
    // Calc should be dropped, since MongoDb supports project push-down and field reordering.
    assertThat(node, instanceOf(BeamPushDownIOSourceRel.class));
    // Only selected fields are projected.
    assertThat(
        node.getRowType().getFieldNames(),
        containsInAnyOrder("c_varchar", "c_boolean", "c_integer"));
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, node);

    assertEquals(expectedSchema, output.getSchema());

    PAssert.that(output).containsInAnyOrder(testRow);

    readPipeline.run().waitUntilFinish();
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
