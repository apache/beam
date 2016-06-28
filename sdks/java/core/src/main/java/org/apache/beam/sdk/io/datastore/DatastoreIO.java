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
package org.apache.beam.sdk.io.datastore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.datastore.V1Beta3.DatastoreSink;
import org.apache.beam.sdk.io.datastore.V1Beta3.DatastoreSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.Key;
import com.google.datastore.v1beta3.Query;
import com.google.protobuf.Int32Value;

import javax.annotation.Nullable;

/**
 * <p>{@link DatastoreIO} provides an API to Read and Write {@link PCollection PCollections} of
 * <a href="https://developers.google.com/datastore/">Google Cloud Datastore</a>
 * {@link Entity} objects.
 *
 * <p>Google Cloud Datastore is a fully managed NoSQL data storage service.
 * An {@code Entity} is an object in Datastore, analogous to a row in traditional
 * database table.
 *
 * <p>This API currently requires an authentication workaround. To use {@link DatastoreIO}, users
 * must use the {@code gcloud} command line tool to get credentials for Datastore:
 * <pre>
 * $ gcloud auth login
 * </pre>
 *
 * <p>To read a {@link PCollection} from a query to Datastore, use {@link DatastoreIO#read} and
 * its methods {@link DatastoreIO.Read#withProjectId} and {@link DatastoreIO.Read#withQuery} to
 * specify the project to query and the query to read from. You can optionally provide a namespace
 * to query within using {@link DatastoreIO.Read#withNamespace}.
 *
 * <p>For example:
 *
 * <pre> {@code
 * // Read a query from Datastore
 * PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
 * Query query = ...;
 * String projectId = "...";
 *
 * Pipeline p = Pipeline.create(options);
 * PCollection<Entity> entities = p.apply(
 *     DatastoreIO.read()
 *         .withProjectId(projectId)
 *         .withQuery(query));
 * } </pre>
 *
 * <p><b>Note:</b> Normally, a Cloud Dataflow job will read from Cloud Datastore in parallel across
 * many workers. However, when the {@link Query} is configured with a limit using
 * {@link com.google.datastore.v1beta3.Query.Builder#setLimit(Int32Value)}, then
 * all returned results will be read by a single Dataflow worker in order to ensure correct data.
 *
 * <p>To write a {@link PCollection} to a Datastore, use {@link DatastoreIO#write},
 * specifying the Cloud datastore project to write to:
 *
 * <pre> {@code
 * PCollection<Entity> entities = ...;
 * entities.apply(DatastoreIO.write().withProjectId(projectId));
 * p.run();
 * } </pre>
 *
 * <p>{@link Entity Entities} in the {@code PCollection} to be written must have complete
 * {@link Key Keys}. Complete {@code Keys} specify the {@code name} and {@code id} of the
 * {@code Entity}, where incomplete {@code Keys} do not. A {@code namespace} other than
 * {@code projectId} default may be used by specifying it in the {@code Entity} {@code Keys}.
 *
 * <pre>{@code
 * Key.Builder keyBuilder = DatastoreHelper.makeKey(...);
 * keyBuilder.getPartitionIdBuilder().setNamespace(namespace);
 * }</pre>
 *
 * <p>{@code Entities} will be committed as upsert (update or insert) mutations. Please read
 * <a href="https://cloud.google.com/datastore/docs/concepts/entities">Entities, Properties, and
 * Keys</a> for more information about {@code Entity} keys.
 *
 * <p><h3>Permissions</h3>
 * Permission requirements depend on the {@code PipelineRunner} that is used to execute the
 * Dataflow job. Please refer to the documentation of corresponding {@code PipelineRunner}s for
 * more details.
 *
 * <p>Please see <a href="https://cloud.google.com/datastore/docs/activate">Cloud Datastore Sign Up
 * </a>for security and permission related information specific to Datastore.
 *
 * @see org.apache.beam.sdk.runners.PipelineRunner
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class DatastoreIO {

  /**
   * Returns an empty {@link DatastoreIO.Read} builder. Configure the source {@code projectId},
   * {@code query}, and optionally {@code namespace} using {@link DatastoreIO.Read#withProjectId},
   * {@link DatastoreIO.Read#withQuery}, and {@link DatastoreIO.Read#withNamespace}.
   */
  public static Read read() {
    return new Read(null, null, null);
  }

  /**
   * A {@link PTransform} that reads the result rows of a Datastore query as {@code Entity}
   * objects.
   *
   * @see DatastoreIO
   */
  public static class Read extends PTransform<PBegin, PCollection<Entity>> {
    @Nullable
    private final String projectId;

    @Nullable
    private final Query query;

    @Nullable
    private final String namespace;

    /**
     * Note that only {@code namespace} is really {@code @Nullable}. The other parameters may be
     * {@code null} as a matter of build order, but if they are {@code null} at instantiation time,
     * an error will be thrown.
     */
    private Read(@Nullable String projectId, @Nullable Query query, @Nullable String namespace) {
      this.projectId = projectId;
      this.query = query;
      this.namespace = namespace;
    }

    /**
     * Returns a new {@link Read} that reads from the Cloud Datastore for the specified project.
     */
    public Read withProjectId(String projectId) {
      checkNotNull(projectId, "projectId");
      return new Read(projectId, query, namespace);
    }

    /**
     * Returns a new {@link Read} that reads the results of the specified query.
     *
     * <p><b>Note:</b> Normally, {@code DatastoreIO} will read from Cloud Datastore in parallel
     * across many workers. However, when the {@link Query} is configured with a limit using
     * {@link Query.Builder#setLimit}, then all results will be read by a single worker in order
     * to ensure correct results.
     */
    public Read withQuery(Query query) {
      checkNotNull(query, "query");
      checkArgument(!query.hasLimit() || query.getLimit().getValue() > 0,
          "Invalid query limit %s: must be positive", query.getLimit().getValue());
      return new Read(projectId, query, namespace);
    }

    /**
     * Returns a new {@link Read} that reads from the given namespace.
     */
    public Read withNamespace(String namespace) {
      return new Read(projectId, query, namespace);
    }

    @Nullable
    public Query getQuery() {
      return query;
    }

    @Nullable
    public String getProjectId() {
      return projectId;
    }

    @Nullable
    public String getNamespace() {
      return namespace;
    }

    @Override
    public PCollection<Entity> apply(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(getSource()));
    }

    @Override
    public void validate(PBegin input) {
      checkNotNull(projectId, "projectId");
      checkNotNull(query, "query");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("projectId", projectId)
              .withLabel("ProjectId"))
          .addIfNotNull(DisplayData.item("namespace", namespace)
              .withLabel("Namespace"))
          .addIfNotNull(DisplayData.item("query", query.toString())
              .withLabel("Query"));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("projectId", projectId)
          .add("query", query)
          .add("namespace", namespace)
          .toString();
    }

    @VisibleForTesting
    DatastoreSource getSource() {
      return new DatastoreSource(projectId, query, namespace);
    }
  }

  /**
   * Returns an empty {@link DatastoreIO.Write} builder. Configure the destination
   * {@code projectId} using {@link DatastoreIO.Write#withProjectId}.
   */
  public static Write write() {
    return new Write(null);
  }

  /**
   * A {@link PTransform} that writes {@link Entity} objects to Cloud Datastore.
   *
   * @see DatastoreIO
   */
  public static class Write extends PTransform<PCollection<Entity>, PDone> {
    @Nullable
    private final String projectId;

    /**
     * Note that {@code projectId} is only {@code @Nullable} as a matter of build order, but if
     * it is {@code null} at instantiation time, an error will be thrown.
     */
    public Write(@Nullable String projectId) {
      this.projectId = projectId;
    }

    /**
     * Returns a new {@link Write} that writes to the Cloud Datastore for the specified project.
     */
    public Write withProjectId(String projectId) {
      checkNotNull(projectId, "projectId");
      return new Write(projectId);
    }

    @Override
    public PDone apply(PCollection<Entity> input) {
      return input.apply(
          org.apache.beam.sdk.io.Write.to(new DatastoreSink(projectId)));
    }

    @Override
    public void validate(PCollection<Entity> input) {
      checkNotNull(projectId, "projectId");
    }

    @Nullable
    public String getProjectId() {
      return projectId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("projectId", projectId)
          .toString();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("projectId", projectId)
              .withLabel("Output Project"));
    }
  }
}
