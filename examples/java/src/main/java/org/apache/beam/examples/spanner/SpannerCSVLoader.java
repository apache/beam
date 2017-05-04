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
package org.apache.beam.examples.spanner;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;



/**
 * Generalized bulk loader for importing CSV files into Spanner.
 *
 */
public class SpannerCSVLoader {

    /**
     * Command options specification.
     */
    private interface Options extends PipelineOptions {
    @Description("Create a sample database")
    @Default.Boolean(false)
    boolean isCreateDatabase();
    void setCreateDatabase(boolean createDatabase);

    @Description("File to read from ")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("Instance ID to write to in Spanner")
    @Validation.Required
    String getInstanceId();
    void setInstanceId(String value);

    @Description("Database ID to write to in Spanner")
    @Validation.Required
    String getDatabaseId();
    void setDatabaseId(String value);

    @Description("Table name")
    @Validation.Required
    String getTable();
    void setTable(String value);
  }


  /**
   * Constructs and executes the processing pipeline based upon command options.
   */
  public static void main(String[] args) throws Exception {
      Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

      Pipeline p = Pipeline.create(options);
      PCollection<String> lines = p.apply(TextIO.Read.from(options.getInput()));
      PCollection<Mutation> mutations = lines
              .apply(ParDo.of(new NaiveParseCsvFn(options.getTable())));
      mutations
              .apply(SpannerIO.writeTo(options.getInstanceId(), options.getDatabaseId()));
      p.run().waitUntilFinish();
  }

  public static void createDatabase(Options options) {
      Spanner client = SpannerOptions.getDefaultInstance().getService();

      DatabaseAdminClient databaseAdminClient = client.getDatabaseAdminClient();
      try {
          databaseAdminClient.dropDatabase(options.getInstanceId(), options
                  .getDatabaseId());
      } catch (SpannerException e) {
          // Does not exist, ignore.
      }
      Operation<Database, CreateDatabaseMetadata> op = databaseAdminClient.createDatabase(
               options.getInstanceId(), options
              .getDatabaseId(), Collections.singleton("CREATE TABLE " + options.getTable() + " ("
              + "  Key           INT64,"
              + "  Name          STRING,"
              + "  Email         STRING,"
              + "  Age           INT,"
              + ") PRIMARY KEY (Key)"));
      op.waitFor();
  }


  /**
   * A DoFn that creates a Spanner Mutation for each CSV line.
   */
  static class NaiveParseCsvFn extends DoFn<String, Mutation> {
      private final String table;

      NaiveParseCsvFn(String table) {
          this.table = table;
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
          String line = c.element();
          String[] elements = line.split(",");
          if (elements.length != 4) {
              return;
          }
          Mutation mutation = Mutation.newInsertOrUpdateBuilder(table)
                  .set("Key").to(Long.valueOf(elements[0]))
                  .set("Name").to(elements[1])
                  .set("Email").to(elements[2])
                  .set("Age").to(Integer.valueOf(elements[3]))
                  .build();
          c.output(mutation);
      }
  }
}
