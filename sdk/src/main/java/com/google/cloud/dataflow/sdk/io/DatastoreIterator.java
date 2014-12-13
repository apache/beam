/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.api.services.datastore.DatastoreV1.EntityResult;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.DatastoreV1.QueryResultBatch;
import com.google.api.services.datastore.DatastoreV1.RunQueryRequest;
import com.google.api.services.datastore.DatastoreV1.RunQueryResponse;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

/**
 * An iterator over the records from a query of the datastore.
 *
 * <p> Usage:
 * <pre>{@code
 *   // Need to pass query and datastore object.
 *   DatastoreIterator iterator = new DatastoreIterator(query, datastore);
 *   while (iterator.hasNext()) {
 *     Entity e = iterator.next().getEntity();
 *     ...
 *   }
 * }</pre>
 */
class DatastoreIterator extends AbstractIterator<EntityResult> {
  /**
   * Query to select records.
   */
  private Query.Builder query;

  /**
   * Datastore to read from.
   */
  private Datastore datastore;

  /**
   * True if more results may be available.
   */
  private boolean moreResults;

  /**
   * Iterator over records.
   */
  private Iterator<EntityResult> entities;

  /**
   * Current batch of query results.
   */
  private QueryResultBatch currentBatch;

  /**
   * Maximum number of results to request per query.
   *
   * <p> Must be set, or it may result in an I/O error when querying
   * Cloud Datastore.
   */
  private static final int QUERY_LIMIT = 5000;

  /**
   * Returns a DatastoreIterator with query and Datastore object set.
   *
   * @param query the query to select records.
   * @param datastore a datastore connection to use.
   */
  public DatastoreIterator(Query query, Datastore datastore) {
    this.query = query.toBuilder().clone();
    this.datastore = datastore;
    this.query.setLimit(QUERY_LIMIT);
  }

  /**
   * Returns an iterator over the next batch of records for the query
   * and updates the cursor to get the next batch as needed.
   * Query has specified limit and offset from InputSplit.
   */
  private Iterator<EntityResult> getIteratorAndMoveCursor()
      throws DatastoreException{
    if (this.currentBatch != null && this.currentBatch.hasEndCursor()) {
      this.query.setStartCursor(this.currentBatch.getEndCursor());
    }

    RunQueryRequest request = RunQueryRequest.newBuilder()
        .setQuery(this.query)
        .build();
    RunQueryResponse response = this.datastore.runQuery(request);

    this.currentBatch = response.getBatch();

    // MORE_RESULTS_AFTER_LIMIT is not implemented yet:
    // https://groups.google.com/forum/#!topic/gcd-discuss/iNs6M1jA2Vw, so
    // use result count to determine if more results might exist.
    int numFetch = this.currentBatch.getEntityResultCount();
    moreResults = numFetch == QUERY_LIMIT;

    // May receive a batch of 0 results if the number of records is a multiple
    // of the request limit.
    if (numFetch == 0) {
      return null;
    }

    return this.currentBatch.getEntityResultList().iterator();
  }

  @Override
  public EntityResult computeNext() {
    try {
      if (entities == null || (!entities.hasNext() && this.moreResults)) {
        entities = getIteratorAndMoveCursor();
      }

      if (entities == null || !entities.hasNext()) {
        return endOfData();
      }

      return entities.next();

    } catch (DatastoreException e) {
      throw new RuntimeException(
          "Datastore error while iterating over entities", e);
    }
  }
}

