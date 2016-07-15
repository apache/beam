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

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.process.runtime.Network;

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
    collection.drop();

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

    PAssert.thatSingleton(output.apply("Count", Count.<String>globally()))
        .isEqualTo(new Long(1000));

    pipeline.run();
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
        .notEqualTo(new Long(1000));

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
