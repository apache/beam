/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.examples.cookbook;

import static com.google.api.services.datastore.client.DatastoreHelper.getPropertyMap;
import static com.google.api.services.datastore.client.DatastoreHelper.getString;
import static com.google.api.services.datastore.client.DatastoreHelper.makeFilter;
import static com.google.api.services.datastore.client.DatastoreHelper.makeKey;
import static com.google.api.services.datastore.client.DatastoreHelper.makeValue;

import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.Property;
import com.google.api.services.datastore.DatastoreV1.PropertyFilter;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.DatastoreV1.Value;
import com.google.cloud.dataflow.examples.WordCount;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.DatastoreIO;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import java.util.Map;
import java.util.UUID;

import javax.annotation.Nullable;

/**
 * A WordCount example using DatastoreIO.
 *
 * <p>This example shows how to use DatastoreIO to read from Datastore and
 * write the results to Cloud Storage.  Note that this example will write
 * data to Datastore, which may incur charge for Datastore operations.
 *
 * <p>To run this example, users need to use gcloud to get credential for Datastore:
 * <pre>{@code
 * $ gcloud auth login
 * }</pre>
 *
 * <p>To run this pipeline locally, the following options must be provided:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --dataset=YOUR_DATASET_ID
 *   --output=[YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PATH]
 * }</pre>
 *
 * <p>To run this example using Dataflow service, you must additionally
 * provide either {@literal --stagingLocation} or {@literal --tempLocation}, and
 * select one of the Dataflow pipeline runners, eg
 * {@literal --runner=BlockingDataflowPipelineRunner}.
 *
 * <p><b>Note:</b> this example creates entities with <i>Ancestor keys</i> to ensure that all
 * entities created are in the same entity group. Similarly, the query used to read from the Cloud
 * Datastore uses an <i>Ancestor filter</i>. Ancestors are used to ensure strongly consistent
 * results in Cloud Datastore. For more information, see the Cloud Datastore documentation on
 * <a href="https://cloud.google.com/datastore/docs/concepts/structuring_for_strong_consistency">
 * Structing Data for Strong Consistency</a>.
 */
public class DatastoreWordCount {

  /**
   * A DoFn that gets the content of an entity (one line in a
   * Shakespeare play) and converts it to a string.
   */
  static class GetContentFn extends DoFn<Entity, String> {
    @Override
    public void processElement(ProcessContext c) {
      Map<String, Value> props = getPropertyMap(c.element());
      Value value = props.get("content");
      if (value != null) {
        c.output(getString(value));
      }
    }
  }

  /**
   * A helper function to create the ancestor key for all created and queried entities.
   *
   * <p>We use ancestor keys and ancestor queries for strong consistency. See
   * {@link DatastoreWordCount} javadoc for more information.
   */
  static Key makeAncestorKey(@Nullable String namespace, String kind) {
    Key.Builder keyBuilder = makeKey(kind, "root");
    if (namespace != null) {
      keyBuilder.getPartitionIdBuilder().setNamespace(namespace);
    }
    return keyBuilder.build();
  }

  /**
   * A DoFn that creates entity for every line in Shakespeare.
   */
  static class CreateEntityFn extends DoFn<String, Entity> {
    private final String namespace;
    private final String kind;
    private final Key ancestorKey;

    CreateEntityFn(String namespace, String kind) {
      this.namespace = namespace;
      this.kind = kind;

      // Build the ancestor key for all created entities once, including the namespace.
      ancestorKey = makeAncestorKey(namespace, kind);
    }

    public Entity makeEntity(String content) {
      Entity.Builder entityBuilder = Entity.newBuilder();

      // All created entities have the same ancestor Key.
      Key.Builder keyBuilder = makeKey(ancestorKey, kind, UUID.randomUUID().toString());
      // NOTE: Namespace is not inherited between keys created with DatastoreHelper.makeKey, so
      // we must set the namespace on keyBuilder. TODO: Once partitionId inheritance is added,
      // we can simplify this code.
      if (namespace != null) {
        keyBuilder.getPartitionIdBuilder().setNamespace(namespace);
      }

      entityBuilder.setKey(keyBuilder.build());
      entityBuilder.addProperty(Property.newBuilder().setName("content")
          .setValue(Value.newBuilder().setStringValue(content)));
      return entityBuilder.build();
    }

    @Override
    public void processElement(ProcessContext c) {
      c.output(makeEntity(c.element()));
    }
  }

  /**
   * Options supported by {@link DatastoreWordCount}.
   *
   * <p>Inherits standard configuration options.
   */
  public static interface Options extends PipelineOptions {
    @Description("Path of the file to read from and store to Datastore")
    @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();
    void setOutput(String value);

    @Description("Dataset ID to read from datastore")
    @Validation.Required
    String getDataset();
    void setDataset(String value);

    @Description("Dataset entity kind")
    @Default.String("shakespeare-demo")
    String getKind();
    void setKind(String value);

    @Description("Dataset namespace")
    String getNamespace();
    void setNamespace(@Nullable String value);

    @Description("Read an existing dataset, do not write first")
    boolean isReadOnly();
    void setReadOnly(boolean value);

    @Description("Number of output shards")
    @Default.Integer(0) // If the system should choose automatically.
    int getNumShards();
    void setNumShards(int value);
  }

  /**
   * An example that creates a pipeline to populate DatastoreIO from a
   * text input.  Forces use of DirectPipelineRunner for local execution mode.
   */
  public static void writeDataToDatastore(Options options) {
      Pipeline p = Pipeline.create(options);
      p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
       .apply(ParDo.of(new CreateEntityFn(options.getNamespace(), options.getKind())))
       .apply(DatastoreIO.writeTo(options.getDataset()));

      p.run();
  }

  /**
   * Build a Cloud Datastore ancestor query for the specified {@link Options#getNamespace} and
   * {@link Options#getKind}.
   *
   * <p>We use ancestor keys and ancestor queries for strong consistency. See
   * {@link DatastoreWordCount} javadoc for more information.
   *
   * @see <a href="https://cloud.google.com/datastore/docs/concepts/queries#Datastore_Ancestor_filters">Ancestor filters</a>
   */
  static Query makeAncestorKindQuery(Options options) {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(options.getKind());
    q.setFilter(makeFilter(
        "__key__",
        PropertyFilter.Operator.HAS_ANCESTOR,
        makeValue(makeAncestorKey(options.getNamespace(), options.getKind()))));
    return q.build();
  }

  /**
   * An example that creates a pipeline to do DatastoreIO.Read from Datastore.
   */
  public static void readDataFromDatastore(Options options) {
    Query query = makeAncestorKindQuery(options);

    // For Datastore sources, the read namespace can be set on the entire query.
    DatastoreIO.Source source = DatastoreIO.source()
        .withDataset(options.getDataset())
        .withQuery(query)
        .withNamespace(options.getNamespace());

    Pipeline p = Pipeline.create(options);
    p.apply("ReadShakespeareFromDatastore", Read.from(source))
        .apply("StringifyEntity", ParDo.of(new GetContentFn()))
        .apply("CountWords", new WordCount.CountWords())
        .apply("PrintWordCount", MapElements.via(new WordCount.FormatAsTextFn()))
        .apply("WriteLines", TextIO.Write.to(options.getOutput())
            .withNumShards(options.getNumShards()));
    p.run();
  }

  /**
   * An example to demo how to use {@link DatastoreIO}.  The runner here is
   * customizable, which means users could pass either {@code DirectPipelineRunner}
   * or {@code DataflowPipelineRunner} in the pipeline options.
   */
  public static void main(String args[]) {
    // The options are used in two places, for Dataflow service, and
    // building DatastoreIO.Read object
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    if (!options.isReadOnly()) {
      // First example: write data to Datastore for reading later.
      //
      // NOTE: this write does not delete any existing Entities in the Datastore, so if run
      // multiple times with the same output dataset, there may be duplicate entries. The
      // Datastore Query tool in the Google Developers Console can be used to inspect or erase all
      // entries with a particular namespace and/or kind.
      DatastoreWordCount.writeDataToDatastore(options);
    }

    // Second example: do parallel read from Datastore.
    DatastoreWordCount.readDataFromDatastore(options);
  }
}
