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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
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
import de.flapdoodle.embed.process.runtime.Network;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test on the MongoDbIO. */
@RunWith(JUnit4.class)
public class MongoDbIOTest {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDbIOTest.class);

  @ClassRule public static final TemporaryFolder MONGODB_LOCATION = new TemporaryFolder();
  private static final String DATABASE = "beam";
  private static final String COLLECTION = "test";

  private static final MongodStarter mongodStarter = MongodStarter.getDefaultInstance();
  private static MongodExecutable mongodExecutable;
  private static MongodProcess mongodProcess;
  private static MongoClient client;

  private static int port;

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws Exception {
    port = NetworkTestHelper.getAvailableLocalPort();
    LOG.info("Starting MongoDB embedded instance on {}", port);
    IMongodConfig mongodConfig =
        new MongodConfigBuilder()
            .version(Version.Main.PRODUCTION)
            .configServer(false)
            .replication(new Storage(MONGODB_LOCATION.getRoot().getPath(), null, 0))
            .net(new Net("localhost", port, Network.localhostIsIPv6()))
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
    client = new MongoClient("localhost", port);

    LOG.info("Insert test data");
    List<Document> documents = createDocuments(1000);
    MongoCollection<Document> collection = getCollection(COLLECTION);
    collection.insertMany(documents);
  }

  @AfterClass
  public static void afterClass() {
    LOG.info("Stopping MongoDB instance");
    client.close();
    mongodProcess.stop();
    mongodExecutable.stop();
  }

  @Test
  public void testSplitIntoFilters() {
    // A single split will result in two filters
    ArrayList<Document> documents = new ArrayList<>();
    documents.add(new Document("_id", 56));
    List<String> filters = MongoDbIO.BoundedMongoDbSource.splitKeysToFilters(documents);
    assertEquals(2, filters.size());
    assertEquals("{ $and: [ {\"_id\":{$lte:ObjectId(\"56\")}} ]}", filters.get(0));
    assertEquals("{ $and: [ {\"_id\":{$gt:ObjectId(\"56\")}} ]}", filters.get(1));

    // Add two more splits; now we should have 4 filters
    documents.add(new Document("_id", 109));
    documents.add(new Document("_id", 256));
    filters = MongoDbIO.BoundedMongoDbSource.splitKeysToFilters(documents);
    assertEquals(4, filters.size());
    assertEquals("{ $and: [ {\"_id\":{$lte:ObjectId(\"56\")}} ]}", filters.get(0));
    assertEquals(
        "{ $and: [ {\"_id\":{$gt:ObjectId(\"56\"),$lte:ObjectId(\"109\")}} ]}", filters.get(1));
    assertEquals(
        "{ $and: [ {\"_id\":{$gt:ObjectId(\"109\"),$lte:ObjectId(\"256\")}} ]}", filters.get(2));
    assertEquals("{ $and: [ {\"_id\":{$gt:ObjectId(\"256\")}} ]}", filters.get(3));
  }

  @Test
  public void testSplitIntoBucket() {
    // a single split should result in two buckets
    ArrayList<Document> documents = new ArrayList<>();
    documents.add(new Document("_id", new ObjectId("52cc8f6254c5317943000005")));
    List<BsonDocument> buckets = MongoDbIO.BoundedMongoDbSource.splitKeysToMatch(documents);
    assertEquals(2, buckets.size());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$lte\": {\"$oid\": \"52cc8f6254c5317943000005\"}}}}",
        buckets.get(0).toString());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$gt\": {\"$oid\": \"52cc8f6254c5317943000005\"}}}}",
        buckets.get(1).toString());

    // add more splits and verify the buckets
    documents.add(new Document("_id", new ObjectId("52cc8f6254c5317943000007")));
    documents.add(new Document("_id", new ObjectId("54242e9e54c531ef8800001f")));
    buckets = MongoDbIO.BoundedMongoDbSource.splitKeysToMatch(documents);
    assertEquals(4, buckets.size());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$lte\": {\"$oid\": \"52cc8f6254c5317943000005\"}}}}",
        buckets.get(0).toString());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$gt\": {\"$oid\": \"52cc8f6254c5317943000005\"}, \"$lte\": {\"$oid\": \"52cc8f6254c5317943000007\"}}}}",
        buckets.get(1).toString());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$gt\": {\"$oid\": \"52cc8f6254c5317943000007\"}, \"$lte\": {\"$oid\": \"54242e9e54c531ef8800001f\"}}}}",
        buckets.get(2).toString());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$gt\": {\"$oid\": \"54242e9e54c531ef8800001f\"}}}}",
        buckets.get(3).toString());
  }

  @Test
  public void testBuildAutoBuckets() {
    List<BsonDocument> aggregates = new ArrayList<BsonDocument>();
    aggregates.add(
        new BsonDocument(
            "$match",
            new BsonDocument("country", new BsonDocument("$eq", new BsonString("England")))));

    MongoDbIO.Read spec =
        MongoDbIO.read()
            .withUri("mongodb://localhost:" + port)
            .withDatabase(DATABASE)
            .withCollection(COLLECTION)
            .withQueryFn(AggregationQuery.create().withMongoDbPipeline(aggregates));
    MongoDatabase database = client.getDatabase(DATABASE);
    List<Document> buckets = MongoDbIO.BoundedMongoDbSource.buildAutoBuckets(database, spec);
    assertEquals(10, buckets.size());
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
                .apply("Map Scientist", MapElements.via(new DocumentToKVFn()))
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
            .withMaxConnectionIdleTime(10)
            .withDatabase(DATABASE)
            .withCollection(COLLECTION);
    assertEquals(10, read.maxConnectionIdleTime());

    PCollection<Document> documents = pipeline.apply(read);

    PAssert.thatSingleton(documents.apply("Count All", Count.globally())).isEqualTo(1000L);

    PAssert.that(
            documents
                .apply("Map Scientist", MapElements.via(new DocumentToKVFn()))
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
                .withQueryFn(FindQuery.create().withFilters(Filters.eq("scientist", "Einstein"))));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(100L);

    pipeline.run();
  }

  @Test
  public void testReadWithFilterAndLimit() throws Exception {
    PCollection<Document> output =
        pipeline.apply(
            MongoDbIO.read()
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withCollection(COLLECTION)
                .withNumSplits(10)
                .withQueryFn(
                    FindQuery.create()
                        .withFilters(Filters.eq("scientist", "Einstein"))
                        .withLimit(5)));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(5L);

    pipeline.run();
  }

  @Test
  public void testReadWithAggregate() throws Exception {
    List<BsonDocument> aggregates = new ArrayList<BsonDocument>();
    aggregates.add(
        new BsonDocument(
            "$match",
            new BsonDocument("country", new BsonDocument("$eq", new BsonString("England")))));

    PCollection<Document> output =
        pipeline.apply(
            MongoDbIO.read()
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withCollection(COLLECTION)
                .withQueryFn(AggregationQuery.create().withMongoDbPipeline(aggregates)));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(300L);

    pipeline.run();
  }

  @Test
  public void testReadWithAggregateWithLimit() throws Exception {
    List<BsonDocument> aggregates = new ArrayList<BsonDocument>();
    aggregates.add(
        new BsonDocument(
            "$match",
            new BsonDocument("country", new BsonDocument("$eq", new BsonString("England")))));
    aggregates.add(new BsonDocument("$limit", new BsonInt32(10)));

    PCollection<Document> output =
        pipeline.apply(
            MongoDbIO.read()
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withCollection(COLLECTION)
                .withQueryFn(AggregationQuery.create().withMongoDbPipeline(aggregates)));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(10L);

    pipeline.run();
  }

  @Test
  public void testWrite() {
    final String collectionName = "testWrite";
    final int numElements = 1000;

    pipeline
        .apply(Create.of(createDocuments(numElements)))
        .apply(
            MongoDbIO.write()
                .withUri("mongodb://localhost:" + port)
                .withDatabase(DATABASE)
                .withCollection(collectionName));

    pipeline.run();

    assertEquals(numElements, countElements(collectionName));
  }

  @Test
  public void testWriteUnordered() {
    final String collectionName = "testWriteUnordered";

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

    assertEquals(1, countElements(collectionName));
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
      document.append("scientist", scientists[index]);
      document.append("country", country[index]);
      documents.add(document);
    }
    return documents;
  }

  private static int countElements(final String collectionName) {
    return Iterators.size(getCollection(collectionName).find().iterator());
  }

  private static MongoCollection<Document> getCollection(final String collectionName) {
    MongoDatabase database = client.getDatabase(DATABASE);
    return database.getCollection(collectionName);
  }

  static class DocumentToKVFn extends SimpleFunction<Document, KV<String, Void>> {
    @Override
    public KV<String, Void> apply(Document input) {
      return KV.of(input.getString("scientist"), null);
    }
  }
}
