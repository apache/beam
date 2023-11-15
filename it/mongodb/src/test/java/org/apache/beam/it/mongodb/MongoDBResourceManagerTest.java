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
package org.apache.beam.it.mongodb;

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
import com.mongodb.client.MongoDatabase;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
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

/** Unit tests for {@link MongoDBResourceManager}. */
@RunWith(JUnit4.class)
public class MongoDBResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private MongoClient mongoClient;

  @Mock private MongoDBContainer container;

  private static final String TEST_ID = "test-id";
  private static final String COLLECTION_NAME = "collection-name";
  private static final String STATIC_DATABASE_NAME = "database";
  private static final String HOST = "localhost";
  private static final int MONGO_DB_PORT = 27017;
  private static final int MAPPED_PORT = 10000;

  private MongoDBResourceManager testManager;

  @Before
  public void setUp() {
    testManager =
        new MongoDBResourceManager(mongoClient, container, MongoDBResourceManager.builder(TEST_ID));
  }

  @Test
  public void testCreateResourceManagerBuilderReturnsMongoDBResourceManager() {
    assertThat(
            MongoDBResourceManager.builder(TEST_ID)
                .useStaticContainer()
                .setHost(HOST)
                .setPort(MONGO_DB_PORT)
                .build())
        .isInstanceOf(MongoDBResourceManager.class);
  }

  @Test
  public void testGetUriShouldReturnCorrectValue() {
    when(container.getMappedPort(MONGO_DB_PORT)).thenReturn(MAPPED_PORT);
    assertThat(
            new MongoDBResourceManager(
                    mongoClient, container, MongoDBResourceManager.builder(TEST_ID))
                .getUri())
        .matches("mongodb://" + HOST + ":" + MAPPED_PORT);
  }

  @Test
  public void testGetDatabaseNameShouldReturnCorrectValue() {
    assertThat(
            new MongoDBResourceManager(
                    mongoClient, container, MongoDBResourceManager.builder(TEST_ID))
                .getDatabaseName())
        .matches(TEST_ID + "-\\d{8}-\\d{6}-\\d{6}");
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
    when(mongoClient.getDatabase(anyString()).listCollectionNames()).thenReturn(null);

    assertThrows(
        MongoDBResourceManagerException.class, () -> testManager.createCollection(COLLECTION_NAME));
  }

  @Test
  public void testCreateCollectionShouldThrowErrorWhenMongoDBFailsToCreateCollection() {
    MongoDatabase mockDatabase = mongoClient.getDatabase(anyString());
    doThrow(IllegalArgumentException.class).when(mockDatabase).createCollection(anyString());

    assertThrows(
        MongoDBResourceManagerException.class, () -> testManager.createCollection(COLLECTION_NAME));
  }

  @Test
  public void testCreateCollectionShouldReturnTrueIfMongoDBDoesNotThrowAnyError() {
    assertThat(testManager.createCollection(COLLECTION_NAME)).isEqualTo(true);
    verify(mongoClient.getDatabase(anyString())).createCollection(anyString());
  }

  @Test
  public void testCreateCollectionShouldReturnFalseIfCollectionAlreadyExists() {
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().hasNext())
        .thenReturn(true, false);
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().next())
        .thenReturn(COLLECTION_NAME);

    assertThat(testManager.createCollection(COLLECTION_NAME)).isEqualTo(false);
    verify(mongoClient.getDatabase(anyString()), never()).getCollection(anyString());
  }

  @Test
  public void testInsertDocumentsShouldCreateCollectionIfOneDoesNotExist() {
    assertThat(testManager.insertDocument(COLLECTION_NAME, new Document())).isEqualTo(true);

    verify(mongoClient.getDatabase(anyString())).getCollection(anyString());
    verify(mongoClient.getDatabase(anyString())).listCollectionNames();
    verify(mongoClient.getDatabase(anyString()).getCollection(anyString())).insertMany(any());
  }

  @Test
  public void
      testInsertDocumentsShouldCreateCollectionIfUsingStaticDatabaseAndCollectionDoesNotExist() {
    MongoDBResourceManager.Builder builder =
        MongoDBResourceManager.builder(TEST_ID).setDatabaseName(STATIC_DATABASE_NAME);
    MongoDBResourceManager tm = new MongoDBResourceManager(mongoClient, container, builder);

    assertThat(tm.insertDocument(COLLECTION_NAME, new Document())).isEqualTo(true);

    verify(mongoClient.getDatabase(anyString())).getCollection(anyString());
    verify(mongoClient.getDatabase(anyString())).listCollectionNames();
    verify(mongoClient.getDatabase(anyString()).getCollection(anyString())).insertMany(any());
  }

  @Test
  public void testInsertDocumentsShouldReturnTrueIfUsingStaticDatabaseAndCollectionDoesExist() {
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().hasNext())
        .thenReturn(true, false);
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().next())
        .thenReturn(COLLECTION_NAME);

    MongoDBResourceManager.Builder builder =
        MongoDBResourceManager.builder(TEST_ID).setDatabaseName(STATIC_DATABASE_NAME);
    MongoDBResourceManager tm = new MongoDBResourceManager(mongoClient, container, builder);

    assertThat(tm.insertDocument(COLLECTION_NAME, new Document())).isEqualTo(true);

    verify(mongoClient.getDatabase(anyString())).getCollection(anyString());
    verify(mongoClient.getDatabase(anyString()).getCollection(anyString())).insertMany(any());
  }

  @Test
  public void testInsertDocumentsShouldThrowErrorWhenMongoDBThrowsException() {
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().hasNext())
        .thenReturn(true, false);
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().next())
        .thenReturn(COLLECTION_NAME);
    MongoCollection<Document> mockCollection =
        mongoClient.getDatabase(anyString()).getCollection(anyString());

    doThrow(MongoBulkWriteException.class)
        .when(mockCollection)
        .insertMany(ImmutableList.of(new Document()));

    assertThrows(
        MongoDBResourceManagerException.class,
        () -> testManager.insertDocument(COLLECTION_NAME, new Document()));
  }

  @Test
  public void testReadCollectionShouldThrowErrorWhenCollectionDoesNotExist() {
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().hasNext())
        .thenReturn(true, false);
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().next())
        .thenReturn("fake-collection-name");

    assertThrows(
        MongoDBResourceManagerException.class, () -> testManager.readCollection(COLLECTION_NAME));
  }

  @Test
  public void testReadCollectionShouldThrowErrorWhenMongoDBFailsToFindDocuments() {
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().hasNext())
        .thenReturn(true, false);
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().next())
        .thenReturn(COLLECTION_NAME);
    MongoCollection<Document> mockCollection =
        mongoClient.getDatabase(anyString()).getCollection(anyString());
    doThrow(RuntimeException.class).when(mockCollection).find();

    assertThrows(
        MongoDBResourceManagerException.class, () -> testManager.readCollection(COLLECTION_NAME));
  }

  @Test
  public void testReadCollectionShouldWorkWhenMongoDBDoesNotThrowAnyError() {
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().hasNext())
        .thenReturn(true, false);
    when(mongoClient.getDatabase(anyString()).listCollectionNames().iterator().next())
        .thenReturn(COLLECTION_NAME);

    testManager.readCollection(COLLECTION_NAME);

    verify(mongoClient.getDatabase(anyString()).getCollection(anyString())).find();
  }

  @Test
  public void testCleanupAllShouldNotDropStaticDatabase() {
    MongoDBResourceManager.Builder builder =
        MongoDBResourceManager.builder(TEST_ID).setDatabaseName(STATIC_DATABASE_NAME);
    MongoDBResourceManager tm = new MongoDBResourceManager(mongoClient, container, builder);

    tm.cleanupAll();

    verify(mongoClient, never()).getDatabase(anyString());
    verify(mongoClient.getDatabase(anyString()), never()).drop();
    verify(mongoClient).close();
  }

  @Test
  public void testCleanupShouldDropNonStaticDatabase() {
    testManager.cleanupAll();

    verify(mongoClient.getDatabase(anyString())).drop();
    verify(mongoClient).close();
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenMongoClientFailsToDropDatabase() {
    MongoDatabase mockDatabase = mongoClient.getDatabase(anyString());
    doThrow(RuntimeException.class).when(mockDatabase).drop();

    assertThrows(MongoDBResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenMongoClientFailsToClose() {
    doThrow(RuntimeException.class).when(mongoClient).close();

    assertThrows(MongoDBResourceManagerException.class, () -> testManager.cleanupAll());
  }
}
