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

import static com.google.cloud.teleport.it.mongodb.MongoDBResourceManagerUtils.checkValidCollectionName;
import static com.google.cloud.teleport.it.mongodb.MongoDBResourceManagerUtils.generateDatabaseName;

import com.google.cloud.teleport.it.testcontainers.TestContainerResourceManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.util.List;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Default class for implementation of {@link MongoDBResourceManager} interface.
 *
 * <p>The class supports one database and multiple collections per database object. A database is
 * created when the first collection is created if one has not been created already.
 *
 * <p>The database name is formed using testId. The database name will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting.
 *
 * <p>The class is thread-safe.
 */
public class DefaultMongoDBResourceManager extends TestContainerResourceManager<GenericContainer<?>>
    implements MongoDBResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMongoDBResourceManager.class);

  private static final String DEFAULT_MONGODB_CONTAINER_NAME = "mongo";

  // A list of available MongoDB Docker image tags can be found at
  // https://hub.docker.com/_/mongo/tags
  private static final String DEFAULT_MONGODB_CONTAINER_TAG = "4.0.18";

  // 27017 is the default port that MongoDB is configured to listen on
  private static final int MONGODB_INTERNAL_PORT = 27017;

  private final MongoClient mongoClient;
  private final String databaseName;
  private final String connectionString;
  private final boolean usingStaticDatabase;

  private DefaultMongoDBResourceManager(DefaultMongoDBResourceManager.Builder builder) {
    this(
        /*mongoClient=*/ null,
        new MongoDBContainer(
            DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag)),
        builder);
  }

  @VisibleForTesting
  DefaultMongoDBResourceManager(
      MongoClient mongoClient,
      MongoDBContainer container,
      DefaultMongoDBResourceManager.Builder builder) {
    super(container, builder);

    this.usingStaticDatabase = builder.databaseName != null;
    this.databaseName =
        usingStaticDatabase ? builder.databaseName : generateDatabaseName(builder.testId);
    this.connectionString =
        String.format("mongodb://%s:%d", this.getHost(), this.getPort(MONGODB_INTERNAL_PORT));
    this.mongoClient = mongoClient == null ? MongoClients.create(connectionString) : mongoClient;
  }

  public static DefaultMongoDBResourceManager.Builder builder(String testId) throws IOException {
    return new DefaultMongoDBResourceManager.Builder(testId);
  }

  @Override
  public synchronized String getUri() {
    return connectionString;
  }

  @Override
  public synchronized String getDatabaseName() {
    return databaseName;
  }

  private synchronized MongoDatabase getDatabase() {
    try {
      return mongoClient.getDatabase(databaseName);
    } catch (Exception e) {
      throw new MongoDBResourceManagerException(
          "Error retrieving database " + databaseName + " from MongoDB.", e);
    }
  }

  private synchronized boolean collectionExists(String collectionName) {
    // Check collection name
    checkValidCollectionName(databaseName, collectionName);

    Iterable<String> collectionNames = getDatabase().listCollectionNames();
    for (String name : collectionNames) {
      // The Collection already exists in the database, return false.
      if (collectionName.equals(name)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public synchronized boolean createCollection(String collectionName) {
    LOG.info("Creating collection using tableName '{}'.", collectionName);

    try {
      // Check to see if the Collection exists
      if (collectionExists(collectionName)) {
        return false;
      }
      // The Collection does not exist in the database, create it and return true.
      getDatabase().getCollection(collectionName);
    } catch (Exception e) {
      throw new MongoDBResourceManagerException("Error creating collection.", e);
    }

    LOG.info("Successfully created collection {}.{}", databaseName, collectionName);

    return true;
  }

  /**
   * Helper method to retrieve a MongoCollection with the given name from the database and return
   * it.
   *
   * @param collectionName The name of the MongoCollection.
   * @param createCollection A boolean that specifies to create the Collection if it does not exist.
   * @return A MongoCollection with the given name.
   */
  private MongoCollection<Document> getMongoDBCollection(
      String collectionName, boolean createCollection) {
    if (!collectionExists(collectionName) && !createCollection) {
      throw new MongoDBResourceManagerException(
          "Collection " + collectionName + " does not exists in database " + databaseName);
    }

    return getDatabase().getCollection(collectionName);
  }

  /**
   * Inserts the given Document into a collection.
   *
   * <p>A database will be created here, if one does not already exist.
   *
   * @param collectionName The name of the collection to insert the document into.
   * @param document The document to insert into the collection.
   * @return A boolean indicating whether the Document was inserted successfully.
   */
  public synchronized boolean insertDocument(String collectionName, Document document) {
    return insertDocuments(collectionName, ImmutableList.of(document));
  }

  @Override
  public synchronized boolean insertDocuments(String collectionName, List<Document> documents) {
    LOG.info(
        "Attempting to write {} documents to {}.{}.",
        documents.size(),
        databaseName,
        collectionName);

    try {
      getMongoDBCollection(collectionName, /*createCollection=*/ true).insertMany(documents);
    } catch (Exception e) {
      throw new MongoDBResourceManagerException("Error inserting documents.", e);
    }

    LOG.info(
        "Successfully wrote {} documents to {}.{}", documents.size(), databaseName, collectionName);

    return true;
  }

  @Override
  public synchronized FindIterable<Document> readCollection(String collectionName) {
    LOG.info("Reading all documents from {}.{}", databaseName, collectionName);

    FindIterable<Document> documents;
    try {
      documents = getMongoDBCollection(collectionName, /*createCollection=*/ false).find();
    } catch (Exception e) {
      throw new MongoDBResourceManagerException("Error reading collection.", e);
    }

    LOG.info("Successfully loaded documents from {}.{}", databaseName, collectionName);

    return documents;
  }

  @Override
  public synchronized boolean cleanupAll() {
    LOG.info("Attempting to cleanup MongoDB manager.");

    boolean producedError = false;

    // First, delete the database if it was not given as a static argument
    try {
      if (!usingStaticDatabase) {
        mongoClient.getDatabase(databaseName).drop();
      }
    } catch (Exception e) {
      LOG.error("Failed to delete MongoDB database {}.", databaseName, e);
      producedError = true;
    }

    // Next, try to close the MongoDB client connection
    try {
      mongoClient.close();
    } catch (Exception e) {
      LOG.error("Failed to delete MongoDB client.", e);
      producedError = true;
    }

    // Throw Exception at the end if there were any errors
    if (producedError || !super.cleanupAll()) {
      throw new MongoDBResourceManagerException(
          "Failed to delete resources. Check above for errors.");
    }

    LOG.info("MongoDB manager successfully cleaned up.");

    return true;
  }

  /** Builder for {@link DefaultMongoDBResourceManager}. */
  public static final class Builder
      extends TestContainerResourceManager.Builder<DefaultMongoDBResourceManager> {

    private String databaseName;

    private Builder(String testId) {
      super(testId);
      this.containerImageName = DEFAULT_MONGODB_CONTAINER_NAME;
      this.containerImageTag = DEFAULT_MONGODB_CONTAINER_TAG;
    }

    /**
     * Sets the database name to that of a static database instance. Use this method only when
     * attempting to operate on a pre-existing Mongo database.
     *
     * <p>Note: if a database name is set, and a static MongoDB server is being used
     * (useStaticContainer() is also called on the builder), then a database will be created on the
     * static server if it does not exist, and it will not be removed when cleanupAll() is called on
     * the MongoDBResourceManager.
     *
     * @param databaseName The database name.
     * @return this builder object with the database name set.
     */
    public Builder setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    @Override
    public DefaultMongoDBResourceManager build() {
      return new DefaultMongoDBResourceManager(this);
    }
  }
}
