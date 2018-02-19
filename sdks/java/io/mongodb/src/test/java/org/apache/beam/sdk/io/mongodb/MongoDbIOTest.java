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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongoCmdOptionsBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.runtime.Network;
import java.io.File;
import java.io.Serializable;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test on the MongoDbIO.
 */
public class MongoDbIOTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbIOTest.class);

  private static final String MONGODB_LOCATION = "target/mongodb";
  private static final String DATABASE = "beam";
  private static final String COLLECTION = "test";

  private static final MongodStarter mongodStarter = MongodStarter.getDefaultInstance();

  private transient MongodExecutable mongodExecutable;
  private transient MongodProcess mongodProcess;

  private static int port;

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  /**
   * Looking for an available network port.
   */
  @BeforeClass
  public static void availablePort() throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      port = serverSocket.getLocalPort();
    }
  }

  @Before
  public void setup() throws Exception {
    LOG.info("Starting MongoDB embedded instance on {}", port);
    try {
      Files.forceDelete(new File(MONGODB_LOCATION));
    } catch (Exception e) {

    }
    new File(MONGODB_LOCATION).mkdirs();
    IMongodConfig mongodConfig = new MongodConfigBuilder()
        .version(Version.Main.PRODUCTION)
        .configServer(false)
        .replication(new Storage(MONGODB_LOCATION, null, 0))
        .net(new Net("localhost", port, Network.localhostIsIPv6()))
        .cmdOptions(new MongoCmdOptionsBuilder()
            .syncDelay(10)
            .useNoPrealloc(true)
            .useSmallFiles(true)
            .useNoJournal(true)
            .build())
        .build();
    mongodExecutable = mongodStarter.prepare(mongodConfig);
    mongodProcess = mongodExecutable.start();

    LOG.info("Insert test data");

    MongoClient client = new MongoClient("localhost", port);
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
    LOG.info("Stopping MongoDB instance");
    mongodProcess.stop();
    mongodExecutable.stop();
  }

  @Test
  public void testSplitIntoFilters() throws Exception {
    ArrayList<Document> documents = new ArrayList<>();
    documents.add(new Document("_id", 56));
    documents.add(new Document("_id", 109));
    documents.add(new Document("_id", 256));
    List<String> filters = MongoDbIO.BoundedMongoDbSource.splitKeysToFilters(documents, null);
    assertEquals(4, filters.size());
    assertEquals("{ $and: [ {\"_id\":{$lte:ObjectId(\"56\")}} ]}", filters.get(0));
    assertEquals("{ $and: [ {\"_id\":{$gt:ObjectId(\"56\"),$lte:ObjectId(\"109\")}} ]}",
        filters.get(1));
    assertEquals("{ $and: [ {\"_id\":{$gt:ObjectId(\"109\"),$lte:ObjectId(\"256\")}} ]}",
        filters.get(2));
    assertEquals("{ $and: [ {\"_id\":{$gt:ObjectId(\"256\")}} ]}", filters.get(3));
  }

  @Test
  public void testFullRead() throws Exception {

    PCollection<Document> output = pipeline.apply(
        MongoDbIO.read()
          .withUri("mongodb://localhost:" + port)
          .withDatabase(DATABASE)
          .withCollection(COLLECTION));

    PAssert.thatSingleton(output.apply("Count All", Count.globally())).isEqualTo(1000L);

    PAssert.that(
            output
                .apply(
                    "Map Scientist",
                    MapElements.via(
                        new SimpleFunction<Document, KV<String, Void>>() {
                          public KV<String, Void> apply(Document input) {
                            return KV.of(input.getString("scientist"), null);
                          }
                        }))
                .apply("Count Scientist", Count.perKey()))
        .satisfies(
            input -> {
              for (KV<String, Long> element : input) {
                assertEquals(100L, element.getValue().longValue());
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testReadWithCustomConnectionOptions() throws Exception {
    MongoDbIO.Read read = MongoDbIO.read()
        .withUri("mongodb://localhost:" + port)
        .withKeepAlive(false)
        .withMaxConnectionIdleTime(10)
        .withDatabase(DATABASE)
        .withCollection(COLLECTION);
    assertFalse(read.keepAlive());
    assertEquals(10, read.maxConnectionIdleTime());

    PCollection<Document> documents = pipeline.apply(read);

    PAssert.thatSingleton(documents.apply("Count All", Count.globally())).isEqualTo(1000L);

    PAssert.that(
            documents
                .apply(
                    "Map Scientist",
                    MapElements.via(
                        new SimpleFunction<Document, KV<String, Void>>() {
                          public KV<String, Void> apply(Document input) {
                            return KV.of(input.getString("scientist"), null);
                          }
                        }))
                .apply("Count Scientist", Count.perKey()))
        .satisfies(
            input -> {
              for (KV<String, Long> element : input) {
                assertEquals(100L, element.getValue().longValue());
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testReadWithFilter() throws Exception {

    PCollection<Document> output = pipeline.apply(
        MongoDbIO.read()
        .withUri("mongodb://localhost:" + port)
        .withDatabase(DATABASE)
        .withCollection(COLLECTION)
        .withFilter("{\"scientist\":\"Einstein\"}"));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(100L);

    pipeline.run();
  }

  @Test
  public void testWrite() throws Exception {

    ArrayList<Document> data = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      data.add(Document.parse(String.format("{\"scientist\":\"Test %s\"}", i)));
    }
    pipeline.apply(Create.of(data))
        .apply(MongoDbIO.write().withUri("mongodb://localhost:" + port).withDatabase("test")
            .withCollection("test"));

    pipeline.run();

    MongoClient client = new MongoClient("localhost", port);
    MongoDatabase database = client.getDatabase("test");
    MongoCollection collection = database.getCollection("test");

    MongoCursor cursor = collection.find().iterator();

    int count = 0;
    while (cursor.hasNext()) {
      count = count + 1;
      cursor.next();
    }

    Assert.assertEquals(10000, count);

  }

  @Test
  public void testWriteEmptyCollection() throws Exception {
    final String emptyCollection = "empty";

    final PCollection<Document> emptyInput =
        pipeline.apply(
            Create.empty(
                SerializableCoder.of(Document.class)));

    emptyInput.apply(
        MongoDbIO.write()
            .withUri("mongodb://localhost:" + port)
            .withDatabase(DATABASE)
            .withCollection(emptyCollection));

    pipeline.run();

    final MongoClient client = new MongoClient("localhost", port);
    final MongoDatabase database = client.getDatabase(DATABASE);
    final MongoCollection collection = database.getCollection(emptyCollection);

    Assert.assertEquals(0, collection.count());
  }
}
