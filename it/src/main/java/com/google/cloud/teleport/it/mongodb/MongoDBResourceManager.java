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

import com.mongodb.client.FindIterable;
import java.util.List;
import org.bson.Document;

/** Interface for managing MongoDB resources in integration tests. */
public interface MongoDBResourceManager {

  /**
   * Returns the name of the Database that this MongoDB manager will operate in.
   *
   * @return the name of the MongoDB Database.
   */
  String getDatabaseName();

  /** Returns the URI connection string to the MongoDB Database. */
  String getUri();

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
  boolean createCollection(String collectionName);

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
  boolean insertDocuments(String collectionName, List<Document> documents);

  /**
   * Reads all the Documents in a collection.
   *
   * @param collectionName The name of the collection to read from.
   * @return An iterable of all the Documents in the collection.
   * @throws MongoDBResourceManagerException if there is an error reading the collection.
   */
  FindIterable<Document> readCollection(String collectionName);

  /**
   * Deletes all created resources (databases, collections and documents) and cleans up the MongoDB
   * client, making the manager object unusable.
   *
   * @throws MongoDBResourceManagerException if there is an error deleting the MongoDB resources.
   */
  boolean cleanupAll();
}
