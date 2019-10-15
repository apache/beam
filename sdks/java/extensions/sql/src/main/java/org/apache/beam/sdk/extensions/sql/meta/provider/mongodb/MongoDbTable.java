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
package org.apache.beam.sdk.extensions.sql.meta.provider.mongodb;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.annotations.VisibleForTesting;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

@Experimental
public class MongoDbTable extends SchemaBaseBeamTable implements Serializable {
  // Should match: mongodb://username:password@localhost:27017/database/collection
  @VisibleForTesting
  final Pattern locationPattern =
      Pattern.compile(
          "(?<credsHostPort>mongodb://(?<usernamePassword>.*(?<password>:.*)?@)?.+:\\d+)/(?<database>.+)/(?<collection>.+)");

  @VisibleForTesting final String dbCollection;
  @VisibleForTesting final String dbName;
  @VisibleForTesting final String dbUri;

  MongoDbTable(Table table) {
    super(table.getSchema());

    String location = table.getLocation();
    Matcher matcher = locationPattern.matcher(location);
    checkArgument(
        matcher.matches(),
        "MongoDb location must be in the following format: 'mongodb://(username:password@)?localhost:27017/database/collection'");
    this.dbUri = matcher.group("credsHostPort"); // "mongodb://localhost:27017"
    this.dbName = matcher.group("database");
    this.dbCollection = matcher.group("collection");
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    // Read MongoDb Documents
    PCollection<Document> readDocuments =
        MongoDbIO.read()
            .withUri(dbUri)
            .withDatabase(dbName)
            .withCollection(dbCollection)
            .expand(begin);

    return readDocuments.apply(DocumentToRow.withSchema(getSchema()));
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException("Writing to a MongoDB is not supported");
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.BOUNDED;
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    long count =
        MongoDbIO.read()
            .withUri(dbUri)
            .withDatabase(dbName)
            .withCollection(dbCollection)
            .getDocumentCount();

    if (count < 0) {
      return BeamTableStatistics.BOUNDED_UNKNOWN;
    }

    return BeamTableStatistics.createBoundedTableStatistics((double) count);
  }

  public static class DocumentToRow extends PTransform<PCollection<Document>, PCollection<Row>> {
    private final Schema schema;

    private DocumentToRow(Schema schema) {
      this.schema = schema;
    }

    public static DocumentToRow withSchema(Schema schema) {
      return new DocumentToRow(schema);
    }

    @Override
    public PCollection<Row> expand(PCollection<Document> input) {
      // TODO(BEAM-8498): figure out a way convert Document directly to Row.
      return input
          .apply("Convert Document to JSON", ParDo.of(new DocumentToJsonStringConverter()))
          .apply("Transform JSON to Row", JsonToRow.withSchema(schema))
          .setRowSchema(schema);
    }

    // TODO: add support for complex fields (May require modifying how Calcite parses nested
    // fields).
    @VisibleForTesting
    static class DocumentToJsonStringConverter extends DoFn<Document, String> {
      @DoFn.ProcessElement
      public void processElement(ProcessContext context) {
        context.output(
            context
                .element()
                .toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build()));
      }
    }
  }
}
