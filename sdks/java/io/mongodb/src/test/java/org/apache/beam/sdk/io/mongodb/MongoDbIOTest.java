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

import static de.flapdoodle.embed.mongo.distribution.Version.Main.PRODUCTION;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.process.runtime.Network;

import java.util.ArrayList;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test on the MongoDbIO.
 */
public class MongoDbIOTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbIOTest.class);

  private static final int PORT = 27017;
  private static final String DATABASE = "beam";
  private static final String COLLECTION = "test";

  private MongodExecutable mongodExecutable;

  @Before
  public void setup() throws Exception {
    LOGGER.info("Starting MongoDB embedded instance");
    IMongodConfig mongodConfig = new MongodConfigBuilder()
        .version(PRODUCTION)
        .net(new Net(PORT, Network.localhostIsIPv6()))
        .build();
    mongodExecutable = MongodStarter.getDefaultInstance().prepare(mongodConfig);
    mongodExecutable.start();

    LOGGER.info("Insert test data");

    MongoClient client = new MongoClient("0.0.0.0", PORT);
    MongoDatabase database = client.getDatabase(DATABASE);

    MongoCollection collection = database.getCollection(COLLECTION);

    String[] scientists = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday",
        "Newton", "Bohr", "Galilei", "Maxwell"};
    for (int i = 1; i <= 1000; i++) {
      int index = i % scientists.length;
      Document document = new Document();
      document.append("_id", i);
      document.append("scientist", scientists[index]);
      collection.insertOne(document);
    }

  }

  @After
  public void stop() throws Exception {
    LOGGER.info("Stopping MongoDB instance");
    mongodExecutable.stop();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFullRead() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    PCollection<String> output = pipeline.apply(
        MongoDbIO.read()
          .withUri("mongodb://localhost:" + PORT)
          .withDatabase(DATABASE)
          .withCollection(COLLECTION));

    PAssert.thatSingleton(output.apply("Count All", Count.<String>globally()))
        .isEqualTo(1000L);

    PAssert.thatSingleton(
        output.apply("Filter Einstein", Filter.by(new ScientistFilter("Einstein")))
        .apply("Count Einstein", Count.<String>globally())).isEqualTo(100L);
    PAssert.thatSingleton(
        output.apply("Filter Darwin", Filter.by(new ScientistFilter("Darwin")))
        .apply("Count Darwin", Count.<String>globally())).isEqualTo(100L);
    PAssert.thatSingleton(
        output.apply("Filter Copernicus", Filter.by(new ScientistFilter("Copernicus")))
        .apply("Count Copernicus", Count.<String>globally())).isEqualTo(100L);
    PAssert.thatSingleton(
        output.apply("Filter Pasteur", Filter.by(new ScientistFilter("Pasteur")))
        .apply("Count Pasteur", Count.<String>globally())).isEqualTo(100L);
    PAssert.thatSingleton(
        output.apply("Filter Curie", Filter.by(new ScientistFilter("Curie")))
        .apply("Count Curie", Count.<String>globally())).isEqualTo(100L);
    PAssert.thatSingleton(
        output.apply("Filter Faraday", Filter.by(new ScientistFilter("Faraday")))
        .apply("Count Faraday", Count.<String>globally())).isEqualTo(100L);
    PAssert.thatSingleton(
        output.apply("Filter Newton", Filter.by(new ScientistFilter("Newton")))
        .apply("Count Newton", Count.<String>globally())).isEqualTo(100L);
    PAssert.thatSingleton(
        output.apply("Filter Bohr", Filter.by(new ScientistFilter("Bohr")))
        .apply("Count Bohr", Count.<String>globally())).isEqualTo(100L);
    PAssert.thatSingleton(
        output.apply("Filter Galilei", Filter.by(new ScientistFilter("Galilei")))
        .apply("Count Galilei", Count.<String>globally())).isEqualTo(100L);
    PAssert.thatSingleton(
        output.apply("Filter Maxwell", Filter.by(new ScientistFilter("Maxwell")))
        .apply("Count Maxwell", Count.<String>globally())).isEqualTo(100L);

    pipeline.run();
  }

  private static class ScientistFilter implements SerializableFunction<String, Boolean> {

    private String scientist;

    public ScientistFilter(String scientist) {
      this.scientist = scientist;
    }

    public Boolean apply(String input) {
      Document bson = Document.parse(input);
      if (bson.getString("scientist").equals(scientist)) {
        return true;
      }
      return false;
    }

  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadWithFilter() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    PCollection<String> output = pipeline.apply(
        MongoDbIO.read()
        .withUri("mongodb://localhost:" + PORT)
        .withDatabase(DATABASE)
        .withCollection(COLLECTION)
        .withFilter("{\"scientist\":\"Einstein\"}"));

    PAssert.thatSingleton(output.apply("Count", Count.<String>globally()))
        .isEqualTo(100L);

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    TestPipeline pipeline = TestPipeline.create();

    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      data.add("{\"scientist\":\"Test\"}");
    }
    pipeline.apply(Create.of(data))
        .apply(MongoDbIO.write().withUri("mongodb://localhost:" + PORT).withDatabase("test")
            .withCollection("test"));

    pipeline.run();

    MongoClient client = new MongoClient("localhost", PORT);
    MongoDatabase database = client.getDatabase("test");
    MongoCollection collection = database.getCollection("test");

    MongoCursor cursor = collection.find().iterator();

    int count = 0;
    while (cursor.hasNext()) {
      count = count + 1;
      cursor.next();
    }

    Assert.assertEquals(count, 100);

  }

}
