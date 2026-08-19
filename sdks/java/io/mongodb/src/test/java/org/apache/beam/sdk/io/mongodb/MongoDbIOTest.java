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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
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
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Test on the MongoDbIO. */
@RunWith(JUnit4.class)
public class MongoDbIOTest {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDbIOTest.class);

  private static final String DATABASE_NAME = "beam";
  private static final String COLLECTION_NAME = "test";
  private static final String VIEW_NAME = "test_view";
  private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:4.4.17");

  @ClassRule
  public static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer(MONGO_IMAGE);

  private static MongoClient client;
  private static MongoDatabase database;

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    LOG.info("Starting MongoDB container");
    client = MongoClients.create(MONGO_CONTAINER.getReplicaSetUrl());
    database = client.getDatabase(DATABASE_NAME);

    LOG.info("Insert test data");
    List<Document> documents = createDocuments(1000, false);
    MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
    collection.insertMany(documents);

    database.createView(VIEW_NAME, COLLECTION_NAME, Collections.emptyList());
  }

  @AfterClass
  public static void afterClass() {
    LOG.info("Stopping MongoDB instance");
    client.close();
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
            .withUri(MONGO_CONTAINER.getReplicaSetUrl())
            .withDatabase(DATABASE_NAME)
            .withCollection(COLLECTION_NAME)
            .withQueryFn(AggregationQuery.create().withMongoDbPipeline(aggregates));

    List<Document> buckets = MongoDbIO.BoundedMongoDbSource.buildAutoBuckets(database, spec);
    assertEquals(10, buckets.size());
  }

  @Test
  public void testFullRead() {
    PCollection<Document> output =
        pipeline.apply(
            MongoDbIO.read()
                .withUri(MONGO_CONTAINER.getReplicaSetUrl())
                .withDatabase(DATABASE_NAME)
                .withCollection(COLLECTION_NAME));

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
  public void testGetSizeCollection() {
    MongoDbIO.BoundedMongoDbSource source =
        new MongoDbIO.BoundedMongoDbSource(
            MongoDbIO.read()
                .withUri(MONGO_CONTAINER.getReplicaSetUrl())
                .withDatabase(DATABASE_NAME)
                .withCollection(COLLECTION_NAME));

    assertThat(source.getEstimatedSizeBytes(pipeline.getOptions()), greaterThan(0L));
  }

  @Test
  public void testGetSizeView() {
    MongoDbIO.BoundedMongoDbSource source =
        new MongoDbIO.BoundedMongoDbSource(
            MongoDbIO.read()
                .withUri(MONGO_CONTAINER.getReplicaSetUrl())
                .withDatabase(DATABASE_NAME)
                .withCollection(VIEW_NAME));

    assertEquals(0, source.getEstimatedSizeBytes(pipeline.getOptions()));
  }

  @Test
  public void testReadWithCustomConnectionOptions() {
    MongoDbIO.Read read =
        MongoDbIO.read()
            .withUri(MONGO_CONTAINER.getReplicaSetUrl())
            .withMaxConnectionIdleTime(10)
            .withDatabase(DATABASE_NAME)
            .withCollection(COLLECTION_NAME);
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
                .withUri(MONGO_CONTAINER.getReplicaSetUrl())
                .withDatabase(DATABASE_NAME)
                .withCollection(COLLECTION_NAME)
                .withQueryFn(FindQuery.create().withFilters(Filters.eq("scientist", "Einstein"))));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(100L);

    pipeline.run();
  }

  @Test
  public void testReadWithFilterAndLimit() throws Exception {
    PCollection<Document> output =
        pipeline.apply(
            MongoDbIO.read()
                .withUri(MONGO_CONTAINER.getReplicaSetUrl())
                .withDatabase(DATABASE_NAME)
                .withCollection(COLLECTION_NAME)
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
                .withUri(MONGO_CONTAINER.getReplicaSetUrl())
                .withDatabase(DATABASE_NAME)
                .withCollection(COLLECTION_NAME)
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
                .withUri(MONGO_CONTAINER.getReplicaSetUrl())
                .withDatabase(DATABASE_NAME)
                .withCollection(COLLECTION_NAME)
                .withQueryFn(AggregationQuery.create().withMongoDbPipeline(aggregates)));

    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(10L);

    pipeline.run();
  }

  @Test
  public void testWrite() {
    final String collectionName = "testWrite";
    final int numElements = 1000;

    pipeline
        .apply(Create.of(createDocuments(numElements, false)))
        .apply(
            MongoDbIO.write()
                .withUri(MONGO_CONTAINER.getReplicaSetUrl())
                .withDatabase(DATABASE_NAME)
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
                .withUri(MONGO_CONTAINER.getReplicaSetUrl())
                .withDatabase(DATABASE_NAME)
                .withOrdered(false)
                .withCollection(collectionName));
    pipeline.run();

    assertEquals(1, countElements(collectionName));
  }

  @Test
  public void testUpdate() {
    final String collectionName = "testUpdate";
    final int numElements = 100;
    Document doc = Document.parse("{\"id\":1,\"scientist\":\"Updated\",\"country\":\"India\"}");

    database.getCollection(collectionName).insertMany(createDocuments(numElements, true));
    assertEquals(numElements, countElements(collectionName));
    List<Document> docs = new ArrayList<>();
    docs.add(doc);
    pipeline
        .apply(Create.of(docs))
        .apply(
            MongoDbIO.write()
                .withUri(MONGO_CONTAINER.getReplicaSetUrl())
                .withDatabase(DATABASE_NAME)
                .withCollection(collectionName)
                .withUpdateConfiguration(
                    UpdateConfiguration.create()
                        .withUpdateKey("id")
                        .withUpdateFields(
                            UpdateField.fieldUpdate("$set", "scientist", "scientist"),
                            UpdateField.fieldUpdate("$set", "country", "country"))));
    pipeline.run();

    Document out = database.getCollection(collectionName).find(new Document("_id", 1)).first();
    assertEquals("Updated", out.get("scientist"));
    assertEquals("India", out.get("country"));
  }

  @Test
  public void testUnknownQueryFnClass() throws IllegalArgumentException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "[org.apache.beam.sdk.io.mongodb.AutoValue_FindQueryTest]" + MongoDbIO.ERROR_MSG_QUERY_FN);

    pipeline.apply(
        MongoDbIO.read()
            .withUri(MONGO_CONTAINER.getReplicaSetUrl())
            .withDatabase(DATABASE_NAME)
            .withCollection(COLLECTION_NAME)
            .withQueryFn(FindQueryTest.create().withFilters(Filters.eq("scientist", "Einstein"))));
  }

  private static List<Document> createDocuments(final int n, boolean addId) {
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
      if (addId) {
        document.append("_id", i);
      }
      document.append("scientist", scientists[index]);
      document.append("country", country[index]);
      documents.add(document);
    }
    return documents;
  }

  private static int countElements(final String collectionName) {
    return Iterators.size(database.getCollection(collectionName).find().iterator());
  }

  static class DocumentToKVFn extends SimpleFunction<Document, KV<String, Void>> {
    @Override
    public KV<String, Void> apply(Document input) {
      return KV.of(input.getString("scientist"), null);
    }
  }
}
