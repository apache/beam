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

import com.google.api.services.datastore.DatastoreV1;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.Property;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.DatastoreV1.Value;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.cloud.dataflow.examples.WordCount;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.DatastoreIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import java.util.Map;
import java.util.UUID;

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
 */
public class DatastoreWordCount {

  /**
   * A DoFn that gets the content of an entity (one line in a
   * Shakespeare play) and converts it to a string.
   */
  static class GetContentFn extends DoFn<Entity, String> {
    @Override
    public void processElement(ProcessContext c) {
      Map<String, Value> props = DatastoreHelper.getPropertyMap(c.element());
      DatastoreV1.Value value = props.get("content");
      if (value != null) {
        c.output(DatastoreHelper.getString(value));
      }
    }
  }

  /**
   * A DoFn that creates entity for every line in Shakespeare.
   */
  static class CreateEntityFn extends DoFn<String, Entity> {
    private String kind;

    CreateEntityFn(String kind) {
      this.kind = kind;
    }

    public Entity makeEntity(String content) {
      Entity.Builder entityBuilder = Entity.newBuilder();
      // Create entities with same ancestor Key.
      Key ancestorKey = DatastoreHelper.makeKey(kind, "root").build();
      Key key = DatastoreHelper.makeKey(ancestorKey, kind, UUID.randomUUID().toString()).build();

      entityBuilder.setKey(key);
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
       .apply(ParDo.of(new CreateEntityFn(options.getKind())))
       .apply(DatastoreIO.writeTo(options.getDataset()));

      p.run();
  }

  /**
   * An example that creates a pipeline to do DatastoreIO.Read from Datastore.
   */
  public static void readDataFromDatastore(Options options) {
    // Build a query: read all entities of the specified kind.
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(options.getKind());
    Query query = q.build();

    Pipeline p = Pipeline.create(options);
    p.apply(DatastoreIO.readFrom(options.getDataset(), query).named("ReadShakespeareFromDatastore"))
     .apply(ParDo.of(new GetContentFn()))
     .apply(new WordCount.CountWords())
     .apply(ParDo.of(new WordCount.FormatAsTextFn()))
     .apply(TextIO.Write.named("WriteLines")
                        .to(options.getOutput())
                        .withNumShards(options.getNumShards()));
    p.run();
  }

  /**
   * Main function.
   * An example to demo how to use DatastoreIO.  The runner here is
   * customizable, which means users could pass either DirectPipelineRunner
   * or DataflowPipelineRunner in PipelineOptions.
   */
  public static void main(String args[]) {
    // The options are used in two places, for Dataflow service, and
    // building DatastoreIO.Read object
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    if (!options.isReadOnly()) {
      // First example: write data to Datastore for reading later.
      // Note: this will insert new entries with the given kind. Existing entries
      // should be cleared first, or the final counts will contain duplicates.
      // The Datastore Admin tool in the AppEngine console can be used to erase
      // all entries with a particular kind.
      DatastoreWordCount.writeDataToDatastore(options);
    }

    // Second example: do parallel read from Datastore.
    DatastoreWordCount.readDataFromDatastore(options);
  }
}
