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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.services.datastore.DatastoreV1.BeginTransactionRequest;
import com.google.api.services.datastore.DatastoreV1.BeginTransactionResponse;
import com.google.api.services.datastore.DatastoreV1.CommitRequest;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.api.services.datastore.client.DatastoreOptions;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.EntityCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.Credentials;
import com.google.cloud.dataflow.sdk.util.RetryHttpRequestInitializer;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PDone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Transforms for reading and writing
 * <a href="https://developers.google.com/datastore/">Google Cloud Datastore</a>
 * entities.
 *
 * <p> The DatastoreIO class provides an experimental API to Read and Write a
 * {@link PCollection} of Datastore Entity.  Currently the class supports
 * read operations on both the DirectPipelineRunner and DataflowPipelineRunner,
 * and write operations on the DirectPipelineRunner.  This API is subject to
 * change, and currently requires an authentication workaround described below.
 *
 * <p> Datastore is a fully managed NoSQL data storage service.
 * An Entity is an object in Datastore, analogous to the a row in traditional
 * database table.  DatastoreIO supports Read/Write from/to Datastore within
 * Dataflow SDK service.
 *
 * <p> To use DatastoreIO, users must set up the environment and use gcloud
 * to get credential for Datastore:
 * <pre>
 * $ export CLOUDSDK_EXTRA_SCOPES=https://www.googleapis.com/auth/datastore
 * $ gcloud auth login
 * </pre>
 *
 * <p> Note that the environment variable CLOUDSDK_EXTRA_SCOPES must be set
 * to the same value when executing a Datastore pipeline, as the local auth
 * cache is keyed by the requested scopes.
 *
 * <p> To read a {@link PCollection} from a query to Datastore, use
 * {@link DatastoreIO.Read}, specifying {@link DatastoreIO.Read#from} to specify
 * dataset to read, the query to read from, and optionally
 * {@link DatastoreIO.Read#named} and {@link DatastoreIO.Read#withHost} to specify
 * the name of the pipeline step and the host of Datastore, respectively.
 * For example:
 *
 * <pre> {@code
 * // Read a query from Datastore
 * PipelineOptions options =
 *     CliPipelineOptionsFactory.create(PipelineOptions.class, args);
 * Pipeline p = Pipeline.create(options);
 * PCollection<Entity> entities =
 *     p.apply(DatastoreIO.Read
 *             .named("Read Datastore")
 *             .from(datasetId, query)
 *             .withHost(host));
 * p.run();
 * } </pre>
 *
 * <p> To write a {@link PCollection} to a datastore, use
 * {@link DatastoreIO.Write}, specifying {@link DatastoreIO.Write#to} to specify
 * the datastore to write to, and optionally {@link TextIO.Write#named} to specify
 * the name of the pipeline step.  For example:
 *
 * <pre> {@code
 * // A simple Write to Datastore with DirectPipelineRunner (writing is not
 * // yet implemented for other runners):
 * PCollection<Entity> entities = ...;
 * lines.apply(DatastoreIO.Write.to("Write entities", datastore));
 * p.run();
 *
 * } </pre>
 */

public class DatastoreIO {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreIO.class);
  private static final String DEFAULT_HOST = "https://www.googleapis.com";

  /**
   * A PTransform that reads from a Datastore query and returns a
   * {@code PCollection<Entity>} containing each of the rows of the table.
   */
  public static class Read {

    /**
     * Returns a DatastoreIO.Read PTransform with the given step name.
     */
    public static Bound named(String name) {
      return new Bound(DEFAULT_HOST).named(name);
    }

    /**
     * Reads entities retrieved from the dataset and a given query.
     */
    public static Bound from(String datasetId, Query query) {
      return new Bound(DEFAULT_HOST).from(datasetId, query);
    }

    /**
     * Returns a DatastoreIO.Read PTransform with specified host.
     */
    public static Bound withHost(String host) {
      return new Bound(host);
    }

    /**
     * A PTransform that reads from a Datastore query and returns a bounded
     * {@code PCollection<Entity>}.
     */
    public static class Bound extends PTransform<PBegin, PCollection<Entity>> {
      String host;
      String datasetId;
      Query query;

      /**
       * Returns a DatastoreIO.Bound object with given query.
       * Sets the name, Datastore host, datasetId, query associated
       * with this PTransform, and options for this Pipeline.
       */
      Bound(String name, String host, String datasetId, Query query) {
        super(name);
        this.host = host;
        this.datasetId = datasetId;
        this.query = query;
      }

      /**
       * Returns a DatastoreIO.Read PTransform with host set up.
       */
      Bound(String host) {
        this.host = host;
      }

      /**
       * Returns a new DatastoreIO.Read PTransform with the name
       * associated with this transformation.
       */
      public Bound named(String name) {
        return new Bound(name, host, datasetId, query);
      }

      /**
       * Returns a new DatastoreIO.Read PTransform with datasetId,
       * and query associated with this transformation, and options
       * associated with this Pipleine.
       */
      public Bound from(String datasetId, Query query) {
        return new Bound(name, host, datasetId, query);
      }

      /**
       * Returns a new DatastoreIO.Read PTransform with the host
       * specified.
       */
      public Bound withHost(String host) {
        return new Bound(name, host, datasetId, query);
      }

      @Override
      public PCollection<Entity> apply(PBegin input) {
        if (datasetId == null || query == null) {
          throw new IllegalStateException(
              "need to set datasetId, and query "
              + "of a DatastoreIO.Read transform");
        }

        QueryOptions queryOptions = QueryOptions.create(host, datasetId, query);
        PCollection<Entity> output;
        try {
          DataflowPipelineOptions options =
              getPipeline().getOptions().as(DataflowPipelineOptions.class);
          PCollection<QueryOptions> queries = splitQueryOptions(queryOptions, options, input);

          output = queries.apply(ParDo.of(new ReadEntitiesFn()));
          getCoderRegistry().registerCoder(Entity.class, EntityCoder.class);
        } catch (DatastoreException e) {
          LOG.warn("DatastoreException: error while doing Datastore query splitting.", e);
          throw new RuntimeException("Error while splitting Datastore query.");
        }

        return output;
      }
    }
  }

  ///////////////////// Write Class /////////////////////////////////
  /**
   * A PTransform that writes a {@code PCollection<Entity>} containing
   * entities to a Datastore kind.
   *
   * Current version only supports Write operation running on
   * DirectPipelineRunner.  If Write is used on DataflowPipelineRunner,
   * it throws UnsupportedOperationException and won't continue on the
   * operation.
   *
   */
  public static class Write {
    /**
     * Returns a DatastoreIO.Write PTransform with the name
     * associated with this PTransform.
     */
    public static Bound named(String name) {
      return new Bound(DEFAULT_HOST).named(name);
    }

    /**
     * Returns a DatastoreIO.Write PTransform with given datasetId.
     */
    public static Bound to(String datasetId) {
      return new Bound(DEFAULT_HOST).to(datasetId);
    }

    /**
     * Returns a DatastoreIO.Write PTransform with specified host.
     */
    public static Bound withHost(String host) {
      return new Bound(host);
    }

    /**
     * A PTransform that writes a bounded {@code PCollection<Entities>}
     * to a Datastore.
     */
    public static class Bound extends PTransform<PCollection<Entity>, PDone> {
      String host;
      String datasetId;

      /**
       * Returns a DatastoreIO.Write PTransform with given host.
       */
      Bound(String host) {
        this.host = host;
      }

      /**
       * Returns a DatastoreIO.Write.Bound object.
       * Sets the name, datastore agent, and kind associated
       * with this transformation.
       */
      Bound(String name, String host, String datasetId) {
        super(name);
        this.host = host;
        this.datasetId = datasetId;
      }

      /**
       * Returns a DatastoreIO.Write PTransform with the name
       * associated with this PTransform.
       */
      public Bound named(String name) {
        return new Bound(name, host, datasetId);
      }

      /**
       * Returns a DatastoreIO.Write PTransform with given datasetId.
       */
      public Bound to(String datasetId) {
        return new Bound(name, host, datasetId);
      }

      /**
       * Returns a new DatastoreIO.Write PTransform with specified host.
       */
      public Bound withHost(String host) {
        return new Bound(name, host, datasetId);
      }

      @Override
      public PDone apply(PCollection<Entity> input) {
        if (this.host == null || this.datasetId == null) {
          throw new IllegalStateException(
              "need to set Datastore host and dataasetId"
              + "of a DatastoreIO.Write transform");
        }

        return new PDone();
      }

      @Override
      protected String getKindString() { return "DatastoreIO.Write"; }

      @Override
      protected Coder<Void> getDefaultOutputCoder() {
        return VoidCoder.of();
      }

      static {
        DirectPipelineRunner.registerDefaultTransformEvaluator(
            Bound.class,
            new DirectPipelineRunner.TransformEvaluator<Bound>() {
              @Override
              public void evaluate(
                  Bound transform,
                  DirectPipelineRunner.EvaluationContext context) {
                evaluateWriteHelper(transform, context);
              }
            });
      }
    }
  }

  ///////////////////////////////////////////////////////////////////

  /**
   * A DoFn that performs query request to Datastore and converts
   * each QueryOptions into Entities.
   */
  private static class ReadEntitiesFn extends DoFn<QueryOptions, Entity> {
    @Override
    public void processElement(ProcessContext c) {
      Query query = c.element().getQuery();
      Datastore datastore = c.element().getWorkerDatastore(
          c.getPipelineOptions().as(GcpOptions.class));
      DatastoreIterator entityIterator = new DatastoreIterator(query, datastore);

      while (entityIterator.hasNext()) {
        c.output(entityIterator.next().getEntity());
      }
    }
  }

  /**
   * A class that stores query and datastore setup environments
   * (host and datasetId).
   */
  @DefaultCoder(AvroCoder.class)
  private static class QueryOptions {
    // Query to read in byte array.
    public byte[] byteQuery;

    // Datastore host to read from.
    public String host;

    // Datastore dataset ID to read from.
    public String datasetId;

    @SuppressWarnings("unused")
    QueryOptions() {}

    /**
     * Returns a QueryOption object without account and private key file
     * (for supporting query on local Datastore).
     *
     * @param host the host of Datastore to connect
     * @param datasetId the dataset ID of Datastore to query
     * @param query the query to perform
     */
    QueryOptions(String host, String datasetId, Query query) {
      this.host = host;
      this.datasetId = datasetId;
      this.setQuery(query);
    }

    /**
     * Creates and returns a QueryOption object for query on local Datastore.
     *
     * @param host the host of Datastore to connect
     * @param datasetId the dataset ID of Datastore to query
     * @param query the query to perform
     */
    public static QueryOptions create(String host, String datasetId, Query query) {
      return new QueryOptions(host, datasetId, query);
    }

    /**
     * Sets up a query.
     * Stores query in a byte array so that we can use AvroCoder to encode/decode
     * QueryOptions.
     *
     * @param q the query to be addressed
     */
    public void setQuery(Query q) {
      this.byteQuery = q.toByteArray();
    }

    /**
     * Returns query.
     *
     * @return query in this option.
     */
    public Query getQuery() {
      try {
        return Query.parseFrom(this.byteQuery);
      } catch (IOException e) {
        LOG.warn("IOException: parsing query failed.", e);
        throw new RuntimeException("Cannot parse query from byte array.");
      }
    }

    /**
     * Returns the dataset ID.
     *
     * @return a dataset ID string for Datastore.
     */
    public String getDatasetId() {
      return this.datasetId;
    }

    /**
     * Returns a copy of QueryOptions from current options with given query.
     *
     * @param query a new query to be set
     * @return A QueryOptions object for query
     */
    public QueryOptions newQuery(Query query) {
      return create(host, datasetId, query);
    }

    /**
     * Returns a Datastore object for connecting to Datastore on workers.
     * This method will try to get worker credential from Credentials
     * library and constructs a Datastore object which is set up and
     * ready to communicate with Datastore.
     *
     * @return a Datastore object setup with host and dataset.
     */
    public Datastore getWorkerDatastore(GcpOptions options) {
      DatastoreOptions.Builder builder = new DatastoreOptions.Builder()
          .host(this.host)
          .dataset(this.datasetId)
          .initializer(new RetryHttpRequestInitializer(null));

      try {
        Credential credential = Credentials.getWorkerCredential(options);
        builder.credential(credential);
      } catch (IOException e) {
        LOG.warn("IOException: can't get credential for worker.", e);
        throw new RuntimeException("Failed on getting credential for worker.");
      }
      return DatastoreFactory.get().create(builder.build());
    }

    /**
     * Returns a Datastore object for connecting to Datastore for users.
     * This method will use the passed in credentials and construct a Datastore
     * object which is set up and ready to communicate with Datastore.
     *
     * @return a Datastore object setup with host and dataset.
     */
    public Datastore getUserDatastore(GcpOptions options) {
      DatastoreOptions.Builder builder = new DatastoreOptions.Builder()
          .host(this.host)
          .dataset(this.datasetId)
          .initializer(new RetryHttpRequestInitializer(null));

      Credential credential = options.getGcpCredential();
      if (credential != null) {
        builder.credential(credential);
      }
      return DatastoreFactory.get().create(builder.build());
    }
  }

  /**
   * Returns a list of QueryOptions by splitting a QueryOptions into sub-queries.
   * This method leverages the QuerySplitter in Datastore to split the
   * query into sub-queries for further parallel query in Dataflow service.
   *
   * @return a PCollection of QueryOptions for split queries
   */
  private static PCollection<QueryOptions> splitQueryOptions(
      QueryOptions queryOptions, DataflowPipelineOptions options,
      PBegin input)
      throws DatastoreException {
    Query query = queryOptions.getQuery();
    Datastore datastore = queryOptions.getUserDatastore(options);

    // Get splits from the QuerySplit interface.
    List<Query> splitQueries = DatastoreHelper.getQuerySplitter()
        .getSplits(query, options.getNumWorkers(), datastore);

    List<PCollection<QueryOptions>> queryList = new LinkedList<>();
    for (Query q : splitQueries) {
      PCollection<QueryOptions> newQuery = input
          .apply(Create.of(queryOptions.newQuery(q)));
      queryList.add(newQuery);
    }

    // This is a workaround to allow for parallelism of a small collection.
    return PCollectionList.of(queryList)
        .apply(Flatten.<QueryOptions>create());
  }

  /////////////////////////////////////////////////////////////////////

  /**
   * Direct mode write evaluator.
   * This writes the result to Datastore.
   */
  private static void evaluateWriteHelper(
      Write.Bound transform,
      DirectPipelineRunner.EvaluationContext context) {
    LOG.info("Writing to Datastore");
    GcpOptions options = context.getPipelineOptions();
    Credential credential = options.getGcpCredential();
    Datastore datastore = DatastoreFactory.get().create(
        new DatastoreOptions.Builder()
            .host(transform.host)
            .dataset(transform.datasetId)
            .credential(credential)
            .initializer(new RetryHttpRequestInitializer(null))
            .build());

    List<Entity> entityList = context.getPCollection(transform.getInput());

    // Create a map to put entities with same ancestor for writing in a batch.
    HashMap<String, List<Entity>> map = new HashMap<>();
    for (Entity e : entityList) {
      String keyOfAncestor = e.getKey().getPathElement(0).getKind()
          + e.getKey().getPathElement(0).getName();
      List<Entity> value = map.get(keyOfAncestor);
      if (value == null) {
        value = new ArrayList<>();
      }
      value.add(e);
      map.put(keyOfAncestor, value);
    }

    // Walk over the map, and write entities bucket by bucket.
    int count = 0;
    for (String k : map.keySet()) {
      List<Entity> entitiesWithSameAncestor = map.get(k);
      List<Entity> toInsert = new ArrayList<>();
      for (Entity e : entitiesWithSameAncestor) {
        toInsert.add(e);
        // Note that Datastore has limit as 500 for a batch operation,
        // so just flush to Datastore with every 500 entties.
        if (toInsert.size() >= 500) {
          writeBatch(toInsert, datastore);
          toInsert.clear();
        }
      }
      writeBatch(toInsert, datastore);
      count += entitiesWithSameAncestor.size();
    }

    LOG.info("Total number of entities written: {}", count);
  }

  /**
   * A function for batch writing to Datastore.
   */
  private static void writeBatch(List<Entity> listOfEntities, Datastore datastore) {
    try {
      BeginTransactionRequest.Builder treq = BeginTransactionRequest.newBuilder();
      BeginTransactionResponse tres = datastore.beginTransaction(treq.build());
      CommitRequest.Builder creq = CommitRequest.newBuilder();
      creq.setTransaction(tres.getTransaction());
      creq.getMutationBuilder().addAllInsertAutoId(listOfEntities);
      datastore.commit(creq.build());
    } catch (DatastoreException e) {
      LOG.warn("Error while doing datastore operation: {}", e);
      throw new RuntimeException("Datastore exception", e);
    }
  }
}
