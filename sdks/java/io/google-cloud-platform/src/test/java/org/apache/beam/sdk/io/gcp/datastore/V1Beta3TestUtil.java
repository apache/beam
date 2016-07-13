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

import static com.google.datastore.v1beta3.QueryResultBatch.MoreResultsType.NOT_FINISHED;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeDelete;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeUpsert;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeValue;

import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.AttemptBoundedExponentialBackOff;
import org.apache.beam.sdk.util.RetryHttpRequestInitializer;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.datastore.v1beta3.CommitRequest;
import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.EntityResult;
import com.google.datastore.v1beta3.Key;
import com.google.datastore.v1beta3.Key.PathElement;
import com.google.datastore.v1beta3.Mutation;
import com.google.datastore.v1beta3.PropertyFilter;
import com.google.datastore.v1beta3.Query;
import com.google.datastore.v1beta3.QueryResultBatch;
import com.google.datastore.v1beta3.RunQueryRequest;
import com.google.datastore.v1beta3.RunQueryResponse;
import com.google.datastore.v1beta3.client.Datastore;
import com.google.datastore.v1beta3.client.DatastoreException;
import com.google.datastore.v1beta3.client.DatastoreFactory;
import com.google.datastore.v1beta3.client.DatastoreOptions;
import com.google.protobuf.Int32Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;

class V1Beta3TestUtil {
  private static final Logger LOG = LoggerFactory.getLogger(V1Beta3TestUtil.class);

  /**
   * A helper function to create the ancestor key for all created and queried entities.
   */
  static Key makeAncestorKey(@Nullable String namespace, String kind, String ancestor) {
    Key.Builder keyBuilder = makeKey(kind, ancestor);
    if (namespace != null) {
      keyBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
    }
    return keyBuilder.build();
  }

  /**
   * Build a datastore ancestor query for the specified kind, namespace and ancestor.
   */
  static Query makeAncestorKindQuery(String kind, @Nullable String namespace, String ancestor) {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(kind);
    q.setFilter(makeFilter(
        "__key__",
        PropertyFilter.Operator.HAS_ANCESTOR,
        makeValue(makeAncestorKey(namespace, kind, ancestor))));
    return q.build();
  }

  /**
   * Build an entity for the given ancestorKey, kind, namespace and value.
   */
  static Entity makeEntity(Long value, Key ancestorKey, String kind, @Nullable String namespace) {
    Entity.Builder entityBuilder = Entity.newBuilder();
    Key.Builder keyBuilder = makeKey(ancestorKey, kind, UUID.randomUUID().toString());
    // NOTE: Namespace is not inherited between keys created with DatastoreHelper.makeKey, so
    // we must set the namespace on keyBuilder. TODO: Once partitionId inheritance is added,
    // we can simplify this code.
    if (namespace != null) {
      keyBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
    }

    entityBuilder.setKey(keyBuilder.build());
    entityBuilder.getMutableProperties().put("value", makeValue(value).build());
    return entityBuilder.build();
  }

  /**
   * A DoFn that creates entity for a long number.
   */
  static class CreateEntityFn extends DoFn<Long, Entity> {
    private final String kind;
    @Nullable
    private final String namespace;
    private Key ancestorKey;

    CreateEntityFn(String kind, @Nullable String namespace, String ancestor) {
      this.kind = kind;
      this.namespace = namespace;
      // Build the ancestor key for all created entities once, including the namespace.
      ancestorKey = makeAncestorKey(namespace, kind, ancestor);
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(makeEntity(c.element(), ancestorKey, kind, namespace));
    }
  }

  /**
   * Build a new datastore client.
   */
  static Datastore getDatastore(PipelineOptions pipelineOptions, String projectId) {
    DatastoreOptions.Builder builder =
        new DatastoreOptions.Builder()
            .projectId(projectId)
            .initializer(
                new RetryHttpRequestInitializer()
            );

    Credential credential = pipelineOptions.as(GcpOptions.class).getGcpCredential();
    if (credential != null) {
      builder.credential(credential);
    }
    return DatastoreFactory.get().create(builder.build());
  }

  /**
   * Build a datastore query request.
   */
  private static RunQueryRequest makeRequest(Query query, @Nullable String namespace) {
    RunQueryRequest.Builder requestBuilder = RunQueryRequest.newBuilder().setQuery(query);
    if (namespace != null) {
      requestBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
    }
    return requestBuilder.build();
  }

  /**
   * Delete all entities with the given ancestor.
   */
  static void deleteAllEntities(V1Beta3TestOptions options, String ancestor) throws Exception {
    Datastore datastore = getDatastore(options, options.getProject());
    Query query = V1Beta3TestUtil.makeAncestorKindQuery(
        options.getKind(), options.getNamespace(), ancestor);

    V1Beta3TestReader reader = new V1Beta3TestReader(datastore, query, options.getNamespace());
    V1Beta3TestWriter writer = new V1Beta3TestWriter(datastore, new DeleteMutationBuilder());

    long numEntities = 0;
    while (reader.advance()) {
      Entity entity = reader.getCurrent();
      numEntities++;
      writer.write(entity);
    }

    writer.close();
    LOG.info("Successfully deleted {} entities", numEntities);
  }

  /**
   * Returns the total number of entities for the given datastore.
   */
  static long countEntities(V1Beta3TestOptions options, String ancestor) throws Exception {
    // Read from datastore.
    Datastore datastore = V1Beta3TestUtil.getDatastore(options, options.getProject());
    Query query = V1Beta3TestUtil.makeAncestorKindQuery(
        options.getKind(), options.getNamespace(), ancestor);

    V1Beta3TestReader reader = new V1Beta3TestReader(datastore, query, options.getNamespace());

    long numEntitiesRead = 0;
    while (reader.advance()) {
      reader.getCurrent();
      numEntitiesRead++;
    }
    return numEntitiesRead;
  }

  /**
   * An interface to represent any datastore mutation operation.
   * Mutation operations include insert, delete, upsert, update.
   */
  interface MutationBuilder {
    Mutation.Builder apply(Entity entity);
  }

  /**
   *A MutationBuilder that performs upsert operation.
   */
  static class UpsertMutationBuilder implements MutationBuilder {
    public Mutation.Builder apply(Entity entity) {
      return makeUpsert(entity);
    }
  }

  /**
   * A MutationBuilder that performs delete operation.
   */
  static class DeleteMutationBuilder implements MutationBuilder {
    public Mutation.Builder apply(Entity entity) {
      return makeDelete(entity.getKey());
    }
  }

  /**
   * A helper class to write entities to datastore.
   */
  static class V1Beta3TestWriter {
    private static final Logger LOG = LoggerFactory.getLogger(V1Beta3TestWriter.class);
    // Limits the number of entities updated per batch
    private static final int DATASTORE_BATCH_UPDATE_LIMIT = 500;
    // Number of times to retry on update failure
    private static final int MAX_RETRIES = 5;
    //Initial backoff time for exponential backoff for retry attempts.
    private static final int INITIAL_BACKOFF_MILLIS = 5000;

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

    private final Datastore datastore;
    private final MutationBuilder mutationBuilder;
    private final List<Entity> entities = new ArrayList<>();

    V1Beta3TestWriter(Datastore datastore, MutationBuilder mutationBuilder) {
      this.datastore = datastore;
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
      BackOff backoff = new AttemptBoundedExponentialBackOff(MAX_RETRIES, INITIAL_BACKOFF_MILLIS);

      while (true) {
        // Batch mutate entities.
        try {
          CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
          for (Entity entity: entities) {
            commitRequest.addMutations(mutationBuilder.apply(entity));
          }
          commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
          datastore.commit(commitRequest.build());
          // Break if the commit threw no exception.
          break;
        } catch (DatastoreException exception) {
          LOG.error("Error writing to the Datastore ({}): {}", exception.getCode(),
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

  /**
   * A helper class to read entities from datastore.
   */
  static class V1Beta3TestReader {
    private static final int QUERY_BATCH_LIMIT = 500;
    private final Datastore datastore;
    private final Query query;
    @Nullable
    private final String namespace;
    private boolean moreResults;
    private java.util.Iterator<EntityResult> entities;
    // Current batch of query results
    private QueryResultBatch currentBatch;
    private Entity currentEntity;

    V1Beta3TestReader(Datastore datastore, Query query, @Nullable String namespace) {
      this.datastore = datastore;
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
      Query.Builder query = this.query.toBuilder().clone();
      query.setLimit(Int32Value.newBuilder().setValue(QUERY_BATCH_LIMIT));
      if (currentBatch != null && !currentBatch.getEndCursor().isEmpty()) {
        query.setStartCursor(currentBatch.getEndCursor());
      }

      RunQueryRequest request = makeRequest(query.build(), namespace);
      RunQueryResponse response = datastore.runQuery(request);

      currentBatch = response.getBatch();

      int numFetch = currentBatch.getEntityResultsCount();
      // All indications from the API are that there are/may be more results.
      moreResults = ((numFetch == QUERY_BATCH_LIMIT)
              || (currentBatch.getMoreResults() == NOT_FINISHED));

      // May receive a batch of 0 results if the number of records is a multiple
      // of the request limit.
      if (numFetch == 0) {
        return null;
      }

      return currentBatch.getEntityResultsList().iterator();
    }
  }
}
