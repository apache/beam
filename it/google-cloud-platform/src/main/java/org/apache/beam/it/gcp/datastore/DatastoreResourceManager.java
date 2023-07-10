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
package org.apache.beam.it.gcp.datastore;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auth.Credentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.GqlQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query.ResultType;
import com.google.cloud.datastore.QueryResults;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;

/** Client for managing Datastore resources. */
public class DatastoreResourceManager implements ResourceManager {

  private final String namespace;

  private final Datastore datastore;
  private final Set<Key> keys;

  public DatastoreResourceManager(Builder builder) {
    this.namespace = builder.namespace;

    this.datastore =
        DatastoreOptions.newBuilder()
            .setProjectId(builder.project)
            .setCredentials(builder.credentials)
            .build()
            .getService();
    this.keys = new HashSet<>();
  }

  @VisibleForTesting
  DatastoreResourceManager(String namespace, Datastore datastore) {
    this.namespace = namespace;
    this.datastore = datastore;
    this.keys = new HashSet<>();
  }

  /**
   * Insert entities to Datastore.
   *
   * @param kind Kind of document to insert.
   * @param entities Entities to insert to Datastore.
   * @return Entities.
   */
  public List<Entity> insert(String kind, Map<Long, FullEntity<?>> entities) {
    List<Entity> created = new ArrayList<>();

    try {
      for (Map.Entry<Long, FullEntity<?>> entry : entities.entrySet()) {
        Key entityKey =
            datastore.newKeyFactory().setKind(kind).setNamespace(namespace).newKey(entry.getKey());
        Entity entity = Entity.newBuilder(entityKey, entry.getValue()).build();
        created.add(datastore.put(entity));
        keys.add(entityKey);
      }
    } catch (Exception e) {
      throw new DatastoreResourceManagerException("Error inserting Datastore entity", e);
    }

    return created;
  }

  /**
   * Run a Gql Query and return the results in entity format.
   *
   * @param gqlQuery Gql Query to run.
   * @return Entities returned from the query.
   */
  public List<Entity> query(String gqlQuery) {
    try {
      QueryResults<Entity> queryResults =
          datastore.run(
              GqlQuery.newGqlQueryBuilder(ResultType.ENTITY, gqlQuery)
                  .setNamespace(namespace)
                  .build());

      List<Entity> entities = new ArrayList<>();

      while (queryResults.hasNext()) {
        Entity entity = queryResults.next();
        entities.add(entity);

        // Mark for deletion if namespace matches the test
        if (entity.getKey().getNamespace().equals(namespace)) {
          keys.add(entity.getKey());
        }
      }
      return entities;
    } catch (Exception e) {
      throw new DatastoreResourceManagerException("Error running Datastore query", e);
    }
  }

  /**
   * Deletes all created entities and cleans up the Datastore client.
   *
   * @throws DatastoreResourceManagerException if there is an error deleting the entities in
   *     Datastore.
   */
  @Override
  public void cleanupAll() throws DatastoreResourceManagerException {
    try {
      datastore.delete(keys.toArray(new Key[0]));
    } catch (Exception e) {
      throw new DatastoreResourceManagerException("Error cleaning up resources", e);
    }
    keys.clear();
  }

  public static Builder builder(String project, String namespace) {
    checkArgument(!Strings.isNullOrEmpty(project), "project can not be empty");
    checkArgument(!Strings.isNullOrEmpty(namespace), "namespace can not be empty");
    return new Builder(project, namespace);
  }

  public static final class Builder {

    private final String project;
    private final String namespace;
    private Credentials credentials;

    private Builder(String project, String namespace) {
      this.project = project;
      this.namespace = namespace;
    }

    public Builder credentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    public DatastoreResourceManager build() {
      if (credentials == null) {
        throw new IllegalArgumentException(
            "Unable to find credentials. Please provide credentials to authenticate to GCP");
      }
      return new DatastoreResourceManager(this);
    }
  }
}
