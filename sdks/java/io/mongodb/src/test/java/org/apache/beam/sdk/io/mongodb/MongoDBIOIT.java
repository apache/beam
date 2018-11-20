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
package org.apache.beam.sdk.io.mongodb;

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.apache.beam.sdk.io.common.IOITHelper.getHashForRecordCount;

import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import java.util.Date;
import java.util.Map;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A test of {@link org.apache.beam.sdk.io.mongodb.MongoDbIO} on an independent Mongo instance.
 *
 * <p>This test requires a running instance of MongoDB. Pass in connection information using
 * PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/mongodb -DintegrationTestPipelineOptions='[
 *  "--mongoDBHostName=1.2.3.4",
 *  "--mongoDBPort=27017",
 *  "--mongoDBDatabaseName=mypass",
 *  "--numberOfRecords=1000" ]'
 *  --tests org.apache.beam.sdk.io.mongodb.MongoDbIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class MongoDBIOIT {

  /** MongoDBIOIT options. */
  public interface MongoDBPipelineOptions extends IOTestPipelineOptions {
    @Description("MongoDB host (host name/ip address)")
    @Default.String("mongodb-host")
    String getMongoDBHostName();

    void setMongoDBHostName(String host);

    @Description("Port for MongoDB")
    @Default.Integer(27017)
    Integer getMongoDBPort();

    void setMongoDBPort(Integer port);

    @Description("Mongo database name")
    @Default.String("beam")
    String getMongoDBDatabaseName();

    void setMongoDBDatabaseName(String name);
  }

  private static final Map<Integer, String> EXPECTED_HASHES =
      ImmutableMap.of(
          1000, "75a0d5803418444e76ae5b421662764c",
          100_000, "3bc762dc1c291904e3c7f577774c6276",
          10_000_000, "e5e0503902018c83e8c8977ef437feba");

  private static MongoDBPipelineOptions options;
  private static String collection;

  @Rule public final TestPipeline writePipeline = TestPipeline.create();
  @Rule public final TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(MongoDBPipelineOptions.class);
    options = TestPipeline.testingPipelineOptions().as(MongoDBPipelineOptions.class);
    collection = String.format("test_%s", new Date().getTime());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    executeWithRetry(MongoDBIOIT::dropDatabase);
  }

  public static void dropDatabase() throws Exception {
    new MongoClient(options.getMongoDBHostName())
        .getDatabase(options.getMongoDBDatabaseName())
        .drop();
  }

  @Test
  public void testWriteAndRead() {
    final String mongoUrl =
        String.format("mongodb://%s:%s", options.getMongoDBHostName(), options.getMongoDBPort());

    writePipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(options.getNumberOfRecords()))
        .apply("Produce documents", MapElements.via(new LongToDocumentFn()))
        .apply(
            "Write documents to MongoDB",
            MongoDbIO.write()
                .withUri(mongoUrl)
                .withDatabase(options.getMongoDBDatabaseName())
                .withCollection(collection));
    writePipeline.run().waitUntilFinish();

    PCollection<String> consolidatedHashcode =
        readPipeline
            .apply(
                "Read all documents",
                MongoDbIO.read()
                    .withUri(mongoUrl)
                    .withDatabase(options.getMongoDBDatabaseName())
                    .withCollection(collection))
            .apply("Map documents to Strings", MapElements.via(new DocumentToStringFn()))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    String expectedHash = getHashForRecordCount(options.getNumberOfRecords(), EXPECTED_HASHES);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    readPipeline.run().waitUntilFinish();
  }

  private static class LongToDocumentFn extends SimpleFunction<Long, Document> {
    @Override
    public Document apply(Long input) {
      return Document.parse(String.format("{\"scientist\":\"Test %s\"}", input));
    }
  }

  private static class DocumentToStringFn extends SimpleFunction<Document, String> {
    @Override
    public String apply(Document input) {
      return input.getString("scientist");
    }
  }
}
