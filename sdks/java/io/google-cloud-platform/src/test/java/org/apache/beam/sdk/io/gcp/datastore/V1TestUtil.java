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
package org.apache.beam.sdk.io.gcp.datastore;

import static com.google.datastore.v1.QueryResultBatch.MoreResultsType.NOT_FINISHED;
import static com.google.datastore.v1.client.DatastoreHelper.makeDelete;
import static com.google.datastore.v1.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeUpsert;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.PathElement;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreOptions;
import com.google.protobuf.Int32Value;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class V1TestUtil {
  private static final Logger LOG = LoggerFactory.getLogger(V1TestUtil.class);

  /** A helper function to create the ancestor key for all created and queried entities. */
  static Key makeAncestorKey(@Nullable String namespace, String kind, String ancestor) {
    Key.Builder keyBuilder = makeKey(kind, ancestor);
    if (namespace != null) {
      keyBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
    }
    return keyBuilder.build();
  }

  /** Build a datastore ancestor query for the specified kind, namespace and ancestor. */
  static Query makeAncestorKindQuery(String kind, @Nullable String namespace, String ancestor) {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(kind);
    q.setFilter(
        makeFilter(
            "__key__",
            PropertyFilter.Operator.HAS_ANCESTOR,
            makeValue(makeAncestorKey(namespace, kind, ancestor))));
    return q.build();
  }

  /**
   * Build an entity for the given ancestorKey, kind, namespace and value.
   *
   * @param largePropertySize if greater than 0, add an unindexed property of the given size.
   */
  static Entity makeEntity(
      Long value, Key ancestorKey, String kind, @Nullable String namespace, int largePropertySize) {
    Entity.Builder entityBuilder = Entity.newBuilder();
    Key.Builder keyBuilder = makeKey(ancestorKey, kind, UUID.randomUUID().toString());
    // NOTE: Namespace is not inherited between keys created with DatastoreHelper.makeKey, so
    // we must set the namespace on keyBuilder. TODO: Once partitionId inheritance is added,
    // we can simplify this code.
    if (namespace != null) {
      keyBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
    }

    entityBuilder.setKey(keyBuilder.build());
    entityBuilder.putProperties("value", makeValue(value).build());
    if (largePropertySize > 0) {
      entityBuilder.putProperties(
          "unindexed_value",
          makeValue(new String(new char[largePropertySize]).replace("\0", "A"))
              .setExcludeFromIndexes(true)
              .build());
    }
    return entityBuilder.build();
  }

  /** A DoFn that creates entity for a long number. */
  static class CreateEntityFn extends DoFn<Long, Entity> {
    private final String kind;
    private final @Nullable String namespace;
    private final int largePropertySize;
    private com.google.datastore.v1.Key ancestorKey;

    CreateEntityFn(
        String kind, @Nullable String namespace, String ancestor, int largePropertySize) {
      this.kind = kind;
      this.namespace = namespace;
      this.largePropertySize = largePropertySize;
      // Build the ancestor key for all created entities once, including the namespace.
      ancestorKey = makeAncestorKey(namespace, kind, ancestor);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(makeEntity(c.element(), ancestorKey, kind, namespace, largePropertySize));
    }
  }

  /** Build a new datastore client. */
  static Datastore getDatastore(
      PipelineOptions pipelineOptions, String projectId, String databaseId) {
    Credentials credential = pipelineOptions.as(GcpOptions.class).getGcpCredential();
    HttpRequestInitializer initializer;
    if (credential != null) {

      initializer =
          new ChainingHttpRequestInitializer(
              new HttpCredentialsAdapter(credential), new RetryHttpRequestInitializer());
    } else {
      initializer = new RetryHttpRequestInitializer();
    }

    DatastoreOptions.Builder builder =
        new DatastoreOptions.Builder().projectId(projectId).initializer(initializer);

    return DatastoreFactory.get().create(builder.build());
  }

  /** Build a datastore query request. */
  private static RunQueryRequest makeRequest(
      String projectId, String databaseId, Query query, @Nullable String namespace) {
    RunQueryRequest.Builder requestBuilder =
        RunQueryRequest.newBuilder()
            .setQuery(query)
            .setProjectId(projectId)
            .setDatabaseId(databaseId);
    if (namespace != null) {
      requestBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
    }
    return requestBuilder.build();
  }

  /** Delete all entities with the given ancestor. */
  static void deleteAllEntities(
      V1TestOptions options, String project, String database, String ancestor) throws Exception {
    Datastore datastore = getDatastore(options, project, database);
    Query query =
        V1TestUtil.makeAncestorKindQuery(options.getKind(), options.getNamespace(), ancestor);

    V1TestReader reader =
        new V1TestReader(datastore, project, database, query, options.getNamespace());
    V1TestWriter writer =
        new V1TestWriter(datastore, project, database, new DeleteMutationBuilder());

    long numEntities = 0;
    while (reader.advance()) {
      Entity entity = reader.getCurrent();
      numEntities++;
      writer.write(entity);
    }

    writer.close();
    LOG.info("Successfully deleted {} entities", numEntities);
  }

  /** Returns the total number of entities for the given datastore. */
  static long countEntities(V1TestOptions options, String project, String database, String ancestor)
      throws Exception {
    // Read from datastore.
    Datastore datastore = V1TestUtil.getDatastore(options, project, database);
    Query query =
        V1TestUtil.makeAncestorKindQuery(options.getKind(), options.getNamespace(), ancestor);

    V1TestReader reader =
        new V1TestReader(datastore, project, database, query, options.getNamespace());

    long numEntitiesRead = 0;
    while (reader.advance()) {
      reader.getCurrent();
      numEntitiesRead++;
    }
    return numEntitiesRead;
  }

  /**
   * An interface to represent any datastore mutation operation. Mutation operations include insert,
   * delete, upsert, update.
   */
  interface MutationBuilder {
    Mutation.Builder apply(Entity entity);
  }

  /** A MutationBuilder that performs upsert operation. */
  static class UpsertMutationBuilder implements MutationBuilder {
    @Override
    public Mutation.Builder apply(Entity entity) {
      return makeUpsert(entity);
    }
  }

  /** A MutationBuilder that performs delete operation. */
  static class DeleteMutationBuilder implements MutationBuilder {
    @Override
    public Mutation.Builder apply(Entity entity) {
      return makeDelete(entity.getKey());
    }
  }

  /** A helper class to write entities to datastore. */
  static class V1TestWriter {
    private static final Logger LOG = LoggerFactory.getLogger(V1TestWriter.class);
    // Limits the number of entities updated per batch
    private static final int DATASTORE_BATCH_UPDATE_LIMIT = 500;
    // Number of times to retry on update failure
    private static final int MAX_RETRIES = 5;
    // Initial backoff time for exponential backoff for retry attempts.
    private static final Duration INITIAL_BACKOFF = Duration.standardSeconds(5);

    // Returns true if a Datastore key is complete. A key is complete if its last element
    // has either an id or a name.
    static boolean isValidKey(Key key) {
      List<PathElement> elementList = key.getPathList();
      if (elementList.isEmpty()) {
        return false;
      }
      PathElement lastElement = elementList.get(elementList.size() - 1);
      return (lastElement.getId() != 0 || !lastElement.getName().isEmpty());
    }

    private final String projectId;
    private final String databaseId;
    private final Datastore datastore;
    private final MutationBuilder mutationBuilder;
    private final List<Entity> entities = new ArrayList<>();

    V1TestWriter(
        Datastore datastore, String projectId, String databaseId, MutationBuilder mutationBuilder) {
      this.datastore = datastore;
      this.projectId = projectId;
      this.databaseId = databaseId;
      this.mutationBuilder = mutationBuilder;
    }

    void write(Entity value) throws Exception {
      // Verify that the entity to write has a complete key.
      if (!isValidKey(value.getKey())) {
        throw new IllegalArgumentException(
            "Entities to be written to the Datastore must have complete keys");
      }

      entities.add(value);

      if (entities.size() >= DATASTORE_BATCH_UPDATE_LIMIT) {
        flushBatch();
      }
    }

    void close() throws Exception {
      // flush any remaining entities
      if (entities.size() > 0) {
        flushBatch();
      }
    }

    // commit the list of entities to datastore
    private void flushBatch() throws DatastoreException, IOException, InterruptedException {
      LOG.info("Writing batch of {} entities", entities.size());
      Sleeper sleeper = Sleeper.DEFAULT;
      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RETRIES)
              .withInitialBackoff(INITIAL_BACKOFF)
              .backoff();

      while (true) {
        // Batch mutate entities.
        try {
          CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
          for (Entity entity : entities) {
            commitRequest.addMutations(mutationBuilder.apply(entity));
          }
          commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
          commitRequest.setProjectId(projectId);
          commitRequest.setDatabaseId(databaseId);
          datastore.commit(commitRequest.build());
          // Break if the commit threw no exception.
          break;
        } catch (DatastoreException exception) {
          LOG.error(
              "Error writing to the Datastore ({}): {}",
              exception.getCode(),
              exception.getMessage());
          if (!BackOffUtils.next(sleeper, backoff)) {
            LOG.error("Aborting after {} retries.", MAX_RETRIES);
            throw exception;
          }
        }
      }
      LOG.info("Successfully wrote {} entities", entities.size());
      entities.clear();
    }
  }

  /** A helper class to read entities from datastore. */
  static class V1TestReader {
    private static final int QUERY_BATCH_LIMIT = 500;
    private final Datastore datastore;
    private final String projectId;
    private final String databaseId;
    private final Query query;
    private final @Nullable String namespace;
    private boolean moreResults;
    private Iterator<EntityResult> entities;
    // Current batch of query results
    private QueryResultBatch currentBatch;
    private Entity currentEntity;

    V1TestReader(
        Datastore datastore,
        String projectId,
        String databaseId,
        Query query,
        @Nullable String namespace) {
      this.datastore = datastore;
      this.projectId = projectId;
      this.databaseId = databaseId;
      this.query = query;
      this.namespace = namespace;
    }

    Entity getCurrent() {
      return currentEntity;
    }

    boolean advance() throws IOException {
      if (entities == null || (!entities.hasNext() && moreResults)) {
        try {
          entities = getIteratorAndMoveCursor();
        } catch (DatastoreException e) {
          throw new IOException(e);
        }
      }

      if (entities == null || !entities.hasNext()) {
        currentEntity = null;
        return false;
      }

      currentEntity = entities.next().getEntity();
      return true;
    }

    // Read the next batch of query results.
    private Iterator<EntityResult> getIteratorAndMoveCursor() throws DatastoreException {
      Query.Builder query = this.query.toBuilder();
      query.setLimit(Int32Value.newBuilder().setValue(QUERY_BATCH_LIMIT));
      if (currentBatch != null && !currentBatch.getEndCursor().isEmpty()) {
        query.setStartCursor(currentBatch.getEndCursor());
      }

      RunQueryRequest request = makeRequest(projectId, databaseId, query.build(), namespace);
      RunQueryResponse response = datastore.runQuery(request);

      currentBatch = response.getBatch();

      int numFetch = currentBatch.getEntityResultsCount();
      // All indications from the API are that there are/may be more results.
      moreResults =
          (numFetch == QUERY_BATCH_LIMIT) || (currentBatch.getMoreResults() == NOT_FINISHED);

      // May receive a batch of 0 results if the number of records is a multiple
      // of the request limit.
      if (numFetch == 0) {
        return null;
      }

      return currentBatch.getEntityResultsList().iterator();
    }
  }
}
