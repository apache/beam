/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.mongodb;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import java.io.IOException;
import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

/** Unit tests for {@link com.google.cloud.teleport.it.mongodb.DefaultMongoDBResourceManager}. */
@RunWith(JUnit4.class)
public class DefaultMongoDBResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private MongoIterable<String> collectionIterable;

  @Mock private MongoClient mongoClient;
  @Mock private MongoDatabase database;
  @Mock private MongoCursor<String> collectionIterator;
  @Mock private MongoCollection<Document> collection;
  @Mock private MongoCursor<String> collectionNames;
  @Mock private MongoDBContainer container;

  private static final String TEST_ID = "test-id";
  private static final String COLLECTION_NAME = "collection-name";
  private static final String STATIC_DATABASE_NAME = "database";
  private static final String HOST = "localhost";
  private static final int MONGO_DB_PORT = 27017;
  private static final int MAPPED_PORT = 10000;

  private DefaultMongoDBResourceManager testManager;

  @Before
  public void setUp() throws IOException, InterruptedException {
    when(container.getHost()).thenReturn(HOST);
    when(container.getMappedPort(MONGO_DB_PORT)).thenReturn(MAPPED_PORT);

    testManager =
        new DefaultMongoDBResourceManager(
            mongoClient, container, DefaultMongoDBResourceManager.builder(TEST_ID));
  }

  @Test
  public void testCreateResourceManagerBuilderReturnsDefaultMongoDBResourceManager()
      throws IOException {
    assertThat(
            DefaultMongoDBResourceManager.builder(TEST_ID)
                .useStaticContainer()
                .setHost(HOST)
                .setPort(MONGO_DB_PORT)
                .build())
        .isInstanceOf(DefaultMongoDBResourceManager.class);
  }

  @Test
  public void testGetUriShouldReturnCorrectValue() {
    assertThat(testManager.getUri()).matches("mongodb://" + HOST + ":" + MAPPED_PORT);
  }

  @Test
  public void testGetDatabaseNameShouldReturnCorrectValue() {
    assertThat(testManager.getDatabaseName()).matches(TEST_ID + "-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testCreateCollectionShouldThrowErrorWhenCollectionNameIsInvalid() {
    assertThrows(
        MongoDBResourceManagerException.class, () -> testManager.createCollection("invalid$name"));
  }

  @Test
  public void testCreateCollectionShouldThrowErrorWhenMongoDBFailsToGetDB() {
    doThrow(IllegalArgumentException.class).when(mongoClient).getDatabase(anyString());

    assertThrows(
        MongoDBResourceManagerException.class, () -> testManager.createCollection(COLLECTION_NAME));
  }

  @Test
  public void testCreateCollectionShouldThrowErrorIfDatabaseFailsToListCollectionNames() {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    when(database.listCollectionNames()).thenReturn(null);

    assertThrows(
        MongoDBResourceManagerException.class, () -> testManager.createCollection(COLLECTION_NAME));
  }

  @Test
  public void testCreateCollectionShouldThrowErrorWhenMongoDBFailsToCreateCollection() {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    when(database.listCollectionNames()).thenReturn(collectionIterable);
    doThrow(IllegalArgumentException.class).when(database).getCollection(anyString());

    assertThrows(
        MongoDBResourceManagerException.class, () -> testManager.createCollection(COLLECTION_NAME));
  }

  @Test
  public void testCreateCollectionShouldReturnTrueIfMongoDBDoesNotThrowAnyError() {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    when(database.listCollectionNames()).thenReturn(collectionIterable);

    assertThat(testManager.createCollection(COLLECTION_NAME)).isEqualTo(true);
    verify(database).getCollection(anyString());
  }

  @Test
  public void testCreateCollectionShouldReturnFalseIfCollectionAlreadyExists() {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    when(database.listCollectionNames()).thenReturn(collectionIterable);
    when(collectionIterable.iterator()).thenReturn(collectionNames);
    when(collectionNames.hasNext()).thenReturn(true, false);
    when(collectionNames.next()).thenReturn(COLLECTION_NAME);

    assertThat(testManager.createCollection(COLLECTION_NAME)).isEqualTo(false);
    verify(database, never()).getCollection(anyString());
  }

  @Test
  public void testInsertDocumentsShouldCreateCollectionIfOneDoesNotExist() {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    when(database.listCollectionNames()).thenReturn(collectionIterable);
    when(database.getCollection(anyString())).thenReturn(collection);

    assertThat(testManager.insertDocument(COLLECTION_NAME, new Document())).isEqualTo(true);

    verify(database).getCollection(anyString());
    verify(database).listCollectionNames();
    verify(collection).insertMany(any());
  }

  @Test
  public void
      testInsertDocumentsShouldCreateCollectionIfUsingStaticDatabaseAndCollectionDoesNotExist()
          throws IOException {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    when(database.listCollectionNames()).thenReturn(collectionIterable);
    when(database.getCollection(anyString())).thenReturn(collection);

    DefaultMongoDBResourceManager.Builder builder =
        DefaultMongoDBResourceManager.builder(TEST_ID).setDatabaseName(STATIC_DATABASE_NAME);
    DefaultMongoDBResourceManager tm =
        new DefaultMongoDBResourceManager(mongoClient, container, builder);

    assertThat(tm.insertDocument(COLLECTION_NAME, new Document())).isEqualTo(true);

    verify(database).getCollection(anyString());
    verify(database).listCollectionNames();
    verify(collection).insertMany(any());
  }

  @Test
  public void testInsertDocumentsShouldReturnTrueIfUsingStaticDatabaseAndCollectionDoesExist()
      throws IOException {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    when(database.listCollectionNames()).thenReturn(collectionIterable);
    when(collectionIterable.iterator()).thenReturn(collectionNames);
    when(collectionNames.hasNext()).thenReturn(true, false);
    when(collectionNames.next()).thenReturn(COLLECTION_NAME);
    when(database.getCollection(anyString())).thenReturn(collection);

    DefaultMongoDBResourceManager.Builder builder =
        DefaultMongoDBResourceManager.builder(TEST_ID).setDatabaseName(STATIC_DATABASE_NAME);
    DefaultMongoDBResourceManager tm =
        new DefaultMongoDBResourceManager(mongoClient, container, builder);

    assertThat(tm.insertDocument(COLLECTION_NAME, new Document())).isEqualTo(true);

    verify(database).getCollection(anyString());
    verify(database).listCollectionNames();
    verify(collection).insertMany(any());
  }

  @Test
  public void testInsertDocumentsShouldThrowErrorWhenMongoDBThrowsException() {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    when(database.listCollectionNames()).thenReturn(collectionIterable);
    when(collectionIterable.iterator()).thenReturn(collectionNames);
    when(collectionNames.hasNext()).thenReturn(true, false);
    when(collectionNames.next()).thenReturn(COLLECTION_NAME);
    when(database.getCollection(anyString())).thenReturn(collection);

    doThrow(MongoBulkWriteException.class)
        .when(collection)
        .insertMany(ImmutableList.of(new Document()));

    assertThrows(
        MongoDBResourceManagerException.class,
        () -> testManager.insertDocument(COLLECTION_NAME, new Document()));
  }

  @Test
  public void testReadCollectionShouldThrowErrorWhenCollectionDoesNotExist() {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    when(database.listCollectionNames()).thenReturn(collectionIterable);
    when(collectionIterable.iterator()).thenReturn(collectionNames);
    when(collectionNames.hasNext()).thenReturn(true, false);
    when(collectionNames.next()).thenReturn("fake-collection-name");

    assertThrows(
        MongoDBResourceManagerException.class, () -> testManager.readCollection(COLLECTION_NAME));
  }

  @Test
  public void testReadCollectionShouldThrowErrorWhenMongoDBFailsToFindDocuments() {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    when(database.listCollectionNames()).thenReturn(collectionIterable);
    when(collectionIterable.iterator()).thenReturn(collectionNames);
    when(collectionNames.hasNext()).thenReturn(true, false);
    when(collectionNames.next()).thenReturn(COLLECTION_NAME);
    when(database.getCollection(anyString())).thenReturn(collection);
    doThrow(RuntimeException.class).when(collection).find();

    assertThrows(
        MongoDBResourceManagerException.class, () -> testManager.readCollection(COLLECTION_NAME));
  }

  @Test
  public void testReadCollectionShouldWorkWhenMongoDBDoesNotThrowAnyError() {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    when(database.listCollectionNames()).thenReturn(collectionIterable);
    when(collectionIterable.iterator()).thenReturn(collectionNames);
    when(collectionNames.hasNext()).thenReturn(true, false);
    when(collectionNames.next()).thenReturn(COLLECTION_NAME);
    when(database.getCollection(anyString())).thenReturn(collection);

    testManager.readCollection(COLLECTION_NAME);

    verify(collection).find();
  }

  @Test
  public void testCleanupAllShouldNotDropStaticDatabase() throws IOException {
    DefaultMongoDBResourceManager.Builder builder =
        DefaultMongoDBResourceManager.builder(TEST_ID).setDatabaseName(STATIC_DATABASE_NAME);
    DefaultMongoDBResourceManager tm =
        new DefaultMongoDBResourceManager(mongoClient, container, builder);

    assertThat(tm.cleanupAll()).isEqualTo(true);

    verify(mongoClient, never()).getDatabase(anyString());
    verify(database, never()).drop();
    verify(mongoClient).close();
  }

  @Test
  public void testCleanupShouldDropNonStaticDatabase() {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);

    assertThat(testManager.cleanupAll()).isEqualTo(true);

    verify(database).drop();
    verify(mongoClient).close();
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenMongoClientFailsToDropDatabase() {
    when(mongoClient.getDatabase(anyString())).thenReturn(database);
    doThrow(RuntimeException.class).when(database).drop();

    assertThrows(MongoDBResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenMongoClientFailsToClose() {
    doThrow(RuntimeException.class).when(mongoClient).close();

    assertThrows(MongoDBResourceManagerException.class, () -> testManager.cleanupAll());
  }
}
