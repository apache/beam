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
import static org.junit.Assert.assertEquals;

import com.mongodb.MongoClient;
import java.util.Arrays;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.io.mongodb.MongoDBIOIT.MongoDBPipelineOptions;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link org.apache.beam.sdk.extensions.sql.meta.provider.mongodb.MongoDbTable} on an
 * independent Mongo instance.
 *
 * <p>This test requires a running instance of MongoDB. Pass in connection information using
 * PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/extensions/sql/integrationTest -DintegrationTestPipelineOptions='[
 *  "--mongoDBHostName=1.2.3.4",
 *  "--mongoDBPort=27017",
 *  "--mongoDBDatabaseName=mypass",
 *  "--numberOfRecords=1000" ]'
 *  --tests org.apache.beam.sdk.extensions.sql.meta.provider.mongodb.MongoDbReadWriteIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * A database, specified in the pipeline options, will be created implicitly if it does not exist
 * already. And dropped upon completing tests.
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class MongoDbReadWriteIT {
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
  private static final String collection = "collection";
  private static MongoDBPipelineOptions options;

  @Rule public final TestPipeline writePipeline = TestPipeline.create();
  @Rule public final TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setUp() throws Exception {
    PipelineOptionsFactory.register(MongoDBPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(MongoDBPipelineOptions.class);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    dropDatabase();
  }

  private static void dropDatabase() throws Exception {
    new MongoClient(options.getMongoDBHostName())
        .getDatabase(options.getMongoDBDatabaseName())
        .drop();
  }

  @Test
  public void testWriteAndRead() {
    final String mongoUrl =
        String.format("mongodb://%s:%d", options.getMongoDBHostName(), options.getMongoDBPort());
    final String mongoSqlUrl =
        String.format(
            "mongodb://%s:%d/%s/%s",
            options.getMongoDBHostName(),
            options.getMongoDBPort(),
            options.getMongoDBDatabaseName(),
            collection);

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

    writePipeline
        .apply(Create.of(testRow))
        .setRowSchema(SOURCE_SCHEMA)
        .apply("Transform Rows to JSON", ToJson.of())
        .apply("Produce documents from JSON", MapElements.via(new ObjectToDocumentFn()))
        .apply(
            "Write documents to MongoDB",
            MongoDbIO.write()
                .withUri(mongoUrl)
                .withDatabase(options.getMongoDBDatabaseName())
                .withCollection(collection));
    PipelineResult writeResult = writePipeline.run();
    writeResult.waitUntilFinish();

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

    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new MongoDbTableProvider());
    sqlEnv.executeDdl(createTableStatement);

    PCollection<Row> output =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery("select * from TEST"));

    assertEquals(output.getSchema(), SOURCE_SCHEMA);

    PAssert.that(output).containsInAnyOrder(testRow);

    readPipeline.run().waitUntilFinish();
  }

  private static class ObjectToDocumentFn extends SimpleFunction<String, Document> {
    @Override
    public Document apply(String input) {
      return Document.parse(input);
    }
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
