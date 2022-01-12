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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.service.AutoService;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;
import java.util.HashMap;

/**
 * An implementation of {@link SchemaIOProvider} for reading and writing to BigQuery with {@link
 * BigQueryIO}. For a description of configuration options, see {@link
 * BigQuerySchemaIOProvider#configurationSchema()}.
 */
@Internal
@Experimental
@AutoService(SchemaIOProvider.class)
public class BigQuerySchemaIOProvider implements SchemaIOProvider {

  /** Returns an id that uniquely represents this IO. */
  @Override
  public String identifier() {
    return "bigquery";
  }

  /**
   * Returns the expected schema of the configuration object. Note this is distinct from the schema
   * of the data source itself. The fields are as follows:
   * <ul>
   *   <li>table: Nullable String - Used for reads and writes. Specifies a table to read or write
   *   to, in the format described in {@link BigQueryHelpers#parseTableSpec}. Used as an input to
   *   {@link BigQueryIO.TypedRead#from(String)} or {@link BigQueryIO.Write#to(String)}.
   *   <li>query: Nullable String - Used for reads. Specifies a query to read results from using the
   *   BigQuery Standard SQL dialect. Used as an input to {@link
   *   BigQueryIO.TypedRead#fromQuery(String)}.
   *   <li>queryLocation: Nullable String - Used for reads. Specifies a BigQuery geographic location
   *   where the query job will be executed. Used as an input to {@link
   *   BigQueryIO.TypedRead#withQueryLocation(String)}.
   *   <li>createDisposition: Nullable String - Used for writes. Specifies whether a table should be
   *   created if it does not exist. Valid inputs are "Never" and "IfNeeded", corresponding to
   *   values of {@link BigQueryIO.Write.CreateDisposition}. Used as an input to
   *   {@link BigQueryIO.Write#withCreateDisposition(BigQueryIO.Write.CreateDisposition)}.
   * </ul>
   */
  @Override
  public Schema configurationSchema() {
    return Schema.builder()
        .addNullableField("table", FieldType.STRING)
        .addNullableField("query", FieldType.STRING)
        .addNullableField("queryLocation", FieldType.STRING)
        .addNullableField("createDisposition", FieldType.STRING)
        .build();
  }

  private static final HashMap<String, BigQueryIO.Write.CreateDisposition> createDispositionsMap =
      new HashMap<>();
  static {
    createDispositionsMap.put("Never", BigQueryIO.Write.CreateDisposition.CREATE_NEVER);
    createDispositionsMap.put("IfNeeded", BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);
  }

  /**
   * Produces a SchemaIO given a String representing the data's location, the schema of the data
   * that resides there, and some IO-specific configuration object.
   *
   * For BigQuery IO, only the configuration object is used. Location and data schema have no
   * effect. Specifying a table and dataset is done through appropriate fields in the configuration
   * object, and the data schema is automatically generated from either the input PCollection or
   * schema of the BigQuery table.
   */
  @Override
  public BigQuerySchemaIO from(String location, Row configuration, Schema dataSchema) {
    return new BigQuerySchemaIO(location, configuration);
  }

  /**
   * Indicates whether this transform requires a specified data schema.
   * @return false
   */
  @Override
  public boolean requiresDataSchema() {
    return false;
  }

  /**
   * Indicates whether the PCollections produced by this transform will contain a bounded or
   * unbounded number of elements.
   * @return Bounded
   */
  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  /** An abstraction to create schema aware IOs. */
  static class BigQuerySchemaIO implements SchemaIO, Serializable {
    protected final Row config;
    protected final String location;

    BigQuerySchemaIO(String location, Row config) {
      this.config = config;
      this.location = location;
    }

    /**
     * Returns the schema of the data, if one was provided.
     * @return null
     */
    @Override
    public Schema schema() {
      return null;
    }

    /**
     * Builds a schema aware reader via {@link BigQueryIO#readTableRowsWithSchema()}.
     */
    @Override
    public PTransform<PBegin, PCollection<Row>> buildReader() {
      return new PTransform<PBegin, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PBegin input) {
          BigQueryIO.TypedRead<TableRow> read = BigQueryIO.readTableRowsWithSchema();
          read = read.withMethod(BigQueryIO.TypedRead.Method.EXPORT);
          if (config.getString("table") != null) {
            read = read.from(config.getString("table"));
          }
          if (config.getString("query") != null) {
            read = read.from(config.getString("query")).usingStandardSql();
          }
          if (config.getString("queryLocation") != null) {
            read = read.withQueryLocation(config.getString("queryLocation"));
          }
          return input.apply(read).apply(Convert.toRows());
        }
      };
    }

    /**
     * Builds a schema aware writer via {@link BigQueryIO#write()}.
     */
    @Override
    public PTransform<PCollection<Row>, PDone> buildWriter() {
      return new PTransform<PCollection<Row>, PDone>() {
        @Override
        public PDone expand(PCollection<Row> input) {
          BigQueryIO.Write<Row> write = BigQueryIO.<Row>write()
              .useBeamSchema()
              .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API);
          if (config.getString("table") != null) {
            write = write.to(config.getString("table"));
          }

          String createDisposition = config.getString("createDisposition");
          if (createDisposition != null && createDispositionsMap.containsKey(createDisposition)) {
            write = write.withCreateDisposition(createDispositionsMap.get(createDisposition));
          }

          input.apply(write);
          return PDone.in(input.getPipeline());
        }
      };
    }
  }
}
