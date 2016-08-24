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
package org.apache.beam.examples.cookbook;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import com.google.datastore.v1.Query;

import javax.annotation.Nullable;

/**
 * An example that deletes entities from Cloud Datastore using DatastoreIO.
 *
 * <p>This example shows how to use DatastoreIO to delete entities from Cloud Datastore. Note that
 * this example will write data to Cloud Datastore, which may incur charge for Cloud Datastore
 * operations.
 *
 * <p>To run this example, users need to use gcloud to get credential for Cloud Datastore:
 * <pre>{@code
 * $ gcloud auth login
 * }</pre>
 *
 * <p>To run this pipeline locally, the following options must be provided:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --kind=YOUR_DATASTORE_KIND_NAME
 * }</pre>
 *
 * <p>To run this example using Dataflow service, you must additionally
 * provide either {@literal --tempLocation} or {@literal --tempLocation}, and
 * select one of the Dataflow pipeline runners, eg
 * {@literal --runner=BlockingDataflowRunner}.
 *
 */
public class DatastoreDelete {
  /**
   * Options supported by {@link DatastoreDelete}.
   *
   * <p>Inherits standard configuration options.
   */
  public static interface Options extends PipelineOptions {
    @Description("Project ID for Cloud Datastore")
    @Validation.Required
    String getProject();
    void setProject(String value);

    @Description("Cloud Datastore Entity kind")
    @Validation.Required
    String getKind();
    void setKind(String value);

    @Description("Cloud Datastore Namespace")
    String getNamespace();
    void setNamespace(@Nullable String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory
        .fromArgs(args).withValidation().as(DatastoreDelete.Options.class);

    // A query to read all entities from the given kind.
    Query.Builder query = Query.newBuilder();
    query.addKindBuilder().setName(options.getKind());

    // A transform to read entities
    DatastoreV1.Read readEntities = DatastoreIO.v1().read()
        .withProjectId(options.getProject())
        .withQuery(query.build())
        .withNamespace(options.getNamespace());

    // A transform to delete entities.
    DatastoreV1.DeleteEntity deleteEntities = DatastoreIO.v1().deleteEntity()
        .withProjectId(options.getProject());

    Pipeline p = Pipeline.create(options);
    p.apply("ReadEntities", readEntities)
        .apply("DeleteEntities", deleteEntities);

    p.run();
  }
}
