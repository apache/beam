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

import static org.apache.beam.it.mongodb.MongoDBResourceManagerUtils.checkValidCollectionName;
import static org.apache.beam.it.mongodb.MongoDBResourceManagerUtils.generateDatabaseName;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.testcontainers.TestContainerResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Client for managing MongoDB resources.
 *
 * <p>The class supports one database and multiple collections per database object. A database is
 * created when the first collection is created if one has not been created already.
 *
 * <p>The database name is formed using testId. The database name will be "{testId}-{ISO8601 time,
 * microsecond precision}", with additional formatting.
 *
 * <p>The class is thread-safe.
 */
public class MongoDBResourceManager extends TestContainerResourceManager<MongoDBContainer>
    implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDBResourceManager.class);

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

  private MongoDBResourceManager(MongoDBResourceManager.Builder builder) {
    this(
        /* mongoClient= */ null,
        new MongoDBContainer(
            DockerImageName.parse(builder.containerImageName).withTag(builder.containerImageTag)),
        builder);
  }

  @VisibleForTesting
  @SuppressWarnings("nullness")
  MongoDBResourceManager(
      @Nullable MongoClient mongoClient,
      MongoDBContainer container,
      MongoDBResourceManager.Builder builder) {
    super(container, builder);

    this.usingStaticDatabase = builder.databaseName != null;
    this.databaseName =
        usingStaticDatabase ? builder.databaseName : generateDatabaseName(builder.testId);
    this.connectionString =
        String.format("mongodb://%s:%d", this.getHost(), this.getPort(MONGODB_INTERNAL_PORT));
    this.mongoClient = mongoClient == null ? MongoClients.create(connectionString) : mongoClient;
  }

  public static MongoDBResourceManager.Builder builder(String testId) {
    return new MongoDBResourceManager.Builder(testId);
  }

  /** Returns the URI connection string to the MongoDB Database. */
  public synchronized String getUri() {
    return connectionString;
  }

  /**
   * Returns the name of the Database that this MongoDB manager will operate in.
   *
   * @return the name of the MongoDB Database.
   */
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

  /**
   * Creates a collection in MongoDB for storing Documents.
   *
   * <p>Note: Implementations may do database creation here, if one does not already exist.
   *
   * @param collectionName Collection name to associate with the given MongoDB instance.
   * @return A boolean indicating whether the resource was created.
   * @throws MongoDBResourceManagerException if there is an error creating the collection in
   *     MongoDB.
   */
  public synchronized boolean createCollection(String collectionName)
      throws MongoDBResourceManagerException {
    LOG.info("Creating collection using collectionName '{}'.", collectionName);

    try {
      // Check to see if the Collection exists
      if (collectionExists(collectionName)) {
        return false;
      }
      // The Collection does not exist in the database, create it and return true.
      getDatabase().createCollection(collectionName);
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

  /**
   * Inserts the given Documents into a collection.
   *
   * <p>Note: Implementations may do collection creation here, if one does not already exist.
   *
   * @param collectionName The name of the collection to insert the documents into.
   * @param documents A list of documents to insert into the collection.
   * @return A boolean indicating whether the Documents were inserted successfully.
   * @throws MongoDBResourceManagerException if there is an error inserting the documents.
   */
  public synchronized boolean insertDocuments(String collectionName, List<Document> documents)
      throws MongoDBResourceManagerException {
    LOG.info(
        "Attempting to write {} documents to {}.{}.",
        documents.size(),
        databaseName,
        collectionName);

    try {
      getMongoDBCollection(collectionName, /* createCollection= */ true).insertMany(documents);
    } catch (Exception e) {
      throw new MongoDBResourceManagerException("Error inserting documents.", e);
    }

    LOG.info(
        "Successfully wrote {} documents to {}.{}", documents.size(), databaseName, collectionName);

    return true;
  }

  /**
   * Reads all the Documents in a collection.
   *
   * @param collectionName The name of the collection to read from.
   * @return A List of all the Documents in the collection.
   * @throws MongoDBResourceManagerException if there is an error reading the collection.
   */
  public synchronized List<Document> readCollection(String collectionName)
      throws MongoDBResourceManagerException {
    LOG.info("Reading all documents from {}.{}", databaseName, collectionName);

    FindIterable<Document> fetchRecords;
    try {
      fetchRecords = getMongoDBCollection(collectionName, /* createCollection= */ false).find();
    } catch (Exception e) {
      throw new MongoDBResourceManagerException("Error reading collection.", e);
    }
    List<Document> documents =
        StreamSupport.stream(fetchRecords.spliterator(), false).collect(Collectors.toList());

    LOG.info("Successfully loaded documents from {}.{}", databaseName, collectionName);

    return documents;
  }

  /**
   * Counts the number of Documents in a collection.
   *
   * @param collectionName The name of the collection to read from.
   * @return The number of Documents in the collection.
   * @throws MongoDBResourceManagerException if there is an error reading the collection.
   */
  public synchronized long countCollection(String collectionName)
      throws MongoDBResourceManagerException {
    LOG.info("Counting all documents from {}.{}", databaseName, collectionName);

    long numDocuments =
        getMongoDBCollection(collectionName, /* createCollection= */ false).countDocuments();

    LOG.info(
        "Successfully counted {} documents from {}.{}", numDocuments, databaseName, collectionName);

    return numDocuments;
  }

  @Override
  public synchronized void cleanupAll() {
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
    if (producedError) {
      throw new MongoDBResourceManagerException(
          "Failed to delete resources. Check above for errors.");
    }

    super.cleanupAll();

    LOG.info("MongoDB manager successfully cleaned up.");
  }

  /** Builder for {@link MongoDBResourceManager}. */
  public static final class Builder
      extends TestContainerResourceManager.Builder<MongoDBResourceManager> {

    private @Nullable String databaseName;

    private Builder(String testId) {
      super(testId, DEFAULT_MONGODB_CONTAINER_NAME, DEFAULT_MONGODB_CONTAINER_TAG);
      this.databaseName = null;
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
    public MongoDBResourceManager build() {
      return new MongoDBResourceManager(this);
    }
  }
}
