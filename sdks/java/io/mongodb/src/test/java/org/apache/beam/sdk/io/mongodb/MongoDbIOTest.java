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

import com.google.common.collect.Iterators;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
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
import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test on the MongoDbIO. */
public class MongoDbIOTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbIOTest.class);

  private static final String MONGODB_LOCATION = "target/mongodb";
  private static final String DATABASE = "beam";
  private static final String COLLECTION = "test";

  private static final MongodStarter mongodStarter = MongodStarter.getDefaultInstance();

  private static transient MongodExecutable mongodExecutable;
  private static transient MongodProcess mongodProcess;

  private static int port;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Looking for an available network port. */
  @BeforeClass
  public static void startServer() throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      port = serverSocket.getLocalPort();
    }
    try {
      Files.forceDelete(new File(MONGODB_LOCATION));
    } catch (Exception e) {
      LOG.error("Could not delete files from existing MongoDB instance.", e);
    }
    boolean mkdirs = new File(MONGODB_LOCATION).mkdirs();
    if (!mkdirs) {
      throw new IOException("Could not create location for embedded MongoDB server0");
    }

    LOG.info("Starting MongoDB embedded instance on {}", port);
    IMongodConfig mongodConfig =
        new MongodConfigBuilder()
            .version(Version.Main.PRODUCTION)
            .configServer(false)
            .replication(new Storage(MONGODB_LOCATION, null, 0))
            .net(new Net("localhost", port, Network.localhostIsIPv6()))
            .cmdOptions(
                new MongoCmdOptionsBuilder()
                    .syncDelay(10)
                    .useNoPrealloc(true)
                    .useSmallFiles(true)
                    .useNoJournal(true)
                    .build())
            .build();
    mongodExecutable = mongodStarter.prepare(mongodConfig);
    mongodProcess = mongodExecutable.start();

    MongoClient client = new MongoClient("localhost", port);
    MongoDatabase database = client.getDatabase(DATABASE);
    MongoCollection collection = database.getCollection(COLLECTION);

    LOG.info("Insert test data");
    List<Document> documents = createDocuments(1000);
    collection.insertMany(documents);
  }

  @AfterClass
  public static void stopServer() {
    LOG.info("Stopping MongoDB instance");
    mongodProcess.stop();
    mongodExecutable.stop();
  }

  private static List<Document> createDocuments(final int n) {
    final String[] scientists =
        new String[] {
          "Einstein",
          "Darwin",
          "Copernicus",
          "Pasteur",
          "Curie",
          "Faraday",
          "Newton",
          "Bohr",
          "Galilei",
          "Maxwell"
        };
    final String[] country =
        new String[] {
          "Germany",
          "England",
          "Poland",
          "France",
          "France",
          "England",
          "England",
          "Denmark",
          "Florence",
          "Scotland"
        };
    List<Document> documents = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      int index = i % scientists.length;
      Document document = new Document();
      document.append("_id", i);
      document.append("scientist", scientists[index]);
      document.append("country", country[index]);
    }
    return documents;
  }

  @Test
  public void testSplitIntoFilters() {
    ArrayList<Document> documents = new ArrayList<>();
    documents.add(new Document("_id", 56));
    documents.add(new Document("_id", 109));
    documents.add(new Document("_id", 256));
    List<String> filters = MongoDbIO.BoundedMongoDbSource.splitKeysToFilters(documents, null);
    assertEquals(4, filters.size());
    assertEquals("{ $and: [ {\"_id\":{$lte:ObjectId(\"56\")}} ]}", filters.get(0));
    assertEquals(
        "{ $and: [ {\"_id\":{$gt:ObjectId(\"56\"),$lte:ObjectId(\"109\")}} ]}", filters.get(1));
    assertEquals(
        "{ $and: [ {\"_id\":{$gt:ObjectId(\"109\"),$lte:ObjectId(\"256\")}} ]}", filters.get(2));
    assertEquals("{ $and: [ {\"_id\":{$gt:ObjectId(\"256\")}} ]}", filters.get(3));
  }

  @Test
  public void testFullRead() {
    PCollection<Document> output =
        pipeline.apply(
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
                          @Override
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
  public void testReadWithCustomConnectionOptions() {
    MongoDbIO.Read read =
        MongoDbIO.read()
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
                          @Override
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
  public void testReadWithFilter() {
    PCollection<Document> output =
        pipeline.apply(
            MongoDbIO.read()
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withCollection(COLLECTION)
                .withFilter("{\"scientist\":\"Einstein\"}"));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(100L);

    pipeline.run();
  }

  @Test
  public void testReadWithFilterAndProjection() throws Exception {

    PCollection<Document> output =
        pipeline.apply(
            MongoDbIO.read()
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withCollection(COLLECTION)
                .withFilter("{\"scientist\":\"Einstein\"}")
                .withProjection("country", "scientist"));

    PAssert.thatSingleton(
            output
                .apply(
                    "Map Scientist",
                    Filter.by(
                        (Document doc) ->
                            doc.get("country") != null && doc.get("scientist") != null))
                .apply("Count", Count.globally()))
        .isEqualTo(100L);

    pipeline.run();
  }

  @Test
  public void testReadWithProjection() {
    PCollection<Document> output =
        pipeline.apply(
            MongoDbIO.read()
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withCollection(COLLECTION)
                .withProjection("country"));

    PAssert.thatSingleton(
            output
                .apply(
                    "Map scientist",
                    Filter.by(
                        (Document doc) ->
                            doc.get("country") != null && doc.get("scientist") == null))
                .apply("Count", Count.globally()))
        .isEqualTo(1000L);

    pipeline.run();
  }

  @Test
  public void testWrite() {
    final String collectionName = "testWrite";
    final int numElements = 10000;

    pipeline
        .apply(Create.of(createDocuments(numElements)))
        .apply(
            MongoDbIO.write()
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withCollection(collectionName));

    pipeline.run();

    MongoClient client = new MongoClient("localhost", port);
    MongoDatabase database = client.getDatabase(DATABASE);
    MongoCollection collection = database.getCollection(collectionName);

    MongoCursor cursor = collection.find().iterator();

    int size = Iterators.size(cursor);
    assertEquals(numElements, size);

  }

  @Test
  public void testWriteEmptyCollection() {
    final String collectionName = "testWriteEmptyCollection";
    final int numElements = 0;

    final PCollection<Document> emptyInput =
        pipeline.apply(Create.empty(SerializableCoder.of(Document.class)));

    emptyInput.apply(
        MongoDbIO.write()
            .withUri("mongodb://localhost:" + port)
            .withDatabase(DATABASE)
            .withCollection(collectionName));

    pipeline.run();

    final MongoClient client = new MongoClient("localhost", port);
    final MongoDatabase database = client.getDatabase(DATABASE);
    final MongoCollection collection = database.getCollection(collectionName);

    Assert.assertEquals(numElements, collection.countDocuments());
  }

  @Test
  public void testWriteUnordered() {
    final String collectionName = "testWriteUnordered";
    final int numElements = 1;

    Document doc =
        Document.parse("{\"_id\":\"521df3a4300466f1f2b5ae82\",\"scientist\":\"Test %s\"}");

    pipeline
        .apply(Create.of(doc, doc))
        .apply(
            MongoDbIO.write()
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withOrdered(false)
                .withCollection(collectionName));
    pipeline.run();

    MongoClient client = new MongoClient("localhost", port);
    MongoDatabase database = client.getDatabase(DATABASE);
    MongoCollection collection = database.getCollection(collectionName);

    MongoCursor cursor = collection.find().iterator();

    int size = Iterators.size(cursor);
    assertEquals(1, size);
  }
}
