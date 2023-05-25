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
import java.io.Serializable;
import java.util.HashMap;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * An implementation of {@link SchemaIOProvider} for reading and writing to BigQuery with {@link
 * BigQueryIO}. For a description of configuration options and other defaults, see {@link
 * BigQuerySchemaIOProvider#configurationSchema()}.
 */
@Internal
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
   *
   * <ul>
   *   <li>table: Nullable String - Used for reads and writes. Specifies a table to read or write
   *       to, in the format described in {@link BigQueryHelpers#parseTableSpec}. Used as an input
   *       to {@link BigQueryIO.TypedRead#from(String)} or {@link BigQueryIO.Write#to(String)}.
   *   <li>query: Nullable String - Used for reads. Specifies a query to read results from using the
   *       BigQuery Standard SQL dialect. Used as an input to {@link
   *       BigQueryIO.TypedRead#fromQuery(String)}.
   *   <li>queryLocation: Nullable String - Used for reads. Specifies a BigQuery geographic location
   *       where the query job will be executed. Used as an input to {@link
   *       BigQueryIO.TypedRead#withQueryLocation(String)}.
   *   <li>createDisposition: Nullable String - Used for writes. Specifies whether a table should be
   *       created if it does not exist. Valid inputs are "Never" and "IfNeeded", corresponding to
   *       values of {@link BigQueryIO.Write.CreateDisposition}. Used as an input to {@link
   *       BigQueryIO.Write#withCreateDisposition(BigQueryIO.Write.CreateDisposition)}.
   * </ul>
   *
   * Relevant default values for these transforms that are not configurable fields are as follows:
   *
   * <ul>
   *   <li>ReadMethod - The input to {@link
   *       BigQueryIO.TypedRead#withMethod(BigQueryIO.TypedRead.Method)}. Defaults to EXPORT, since
   *       that is the only method that currently offers Beam Schema support.
   *   <li>WriteMethod - The input to {@link BigQueryIO.Write#withMethod(BigQueryIO.Write.Method)}.
   *       Currently defaults to STORAGE_WRITE_API.
   * </ul>
   */
  @Override
  public Schema configurationSchema() {
    return Schema.builder()
        .addNullableField("table", FieldType.STRING)
        .addNullableField("query", FieldType.STRING)
        .addNullableField("queryLocation", FieldType.STRING)
        .addNullableField("createDisposition", FieldType.STRING)
        .addNullableField("useTestingBigQueryServices", FieldType.BOOLEAN)
        .addNullableField("autoSharding", FieldType.BOOLEAN)
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
   * <p>For BigQuery IO, only the configuration object is used. Location and data schema have no
   * effect. Specifying a table and dataset is done through appropriate fields in the configuration
   * object, and the data schema is automatically generated from either the input PCollection or
   * schema of the BigQuery table.
   */
  @Override
  public BigQuerySchemaIO from(String location, Row configuration, @Nullable Schema dataSchema) {
    return new BigQuerySchemaIO(location, configuration);
  }

  /**
   * Indicates whether this transform requires a specified data schema.
   *
   * @return false
   */
  @Override
  public boolean requiresDataSchema() {
    return false;
  }

  /**
   * Indicates whether the PCollections produced by this transform will contain a bounded or
   * unbounded number of elements.
   *
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
     *
     * @return null
     */
    @Override
    @SuppressWarnings({
      "nullness" // TODO(https://github.com/apache/beam/issues/20497)
    })
    public Schema schema() {
      return null;
    }

    /** Builds a schema aware reader via {@link BigQueryIO#readTableRowsWithSchema()}. */
    @Override
    public PTransform<PBegin, PCollection<Row>> buildReader() {
      return new PTransform<PBegin, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PBegin input) {
          BigQueryIO.TypedRead<TableRow> read = BigQueryIO.readTableRowsWithSchema();
          read = read.withMethod(BigQueryIO.TypedRead.Method.EXPORT);

          String table = config.getString("table");
          if (table != null) {
            read = read.from(table);
          }
          String query = config.getString("query");
          if (query != null) {
            read = read.fromQuery(query).usingStandardSql();
          }
          String queryLocation = config.getString("queryLocation");
          if (queryLocation != null) {
            read = read.withQueryLocation(queryLocation);
          }
          return input.apply(read).apply(Convert.toRows());
        }
      };
    }

    /** Builds a schema aware writer via {@link BigQueryIO#write()}. */
    @Override
    public PTransform<PCollection<Row>, PDone> buildWriter() {
      return new PTransform<PCollection<Row>, PDone>() {
        @Override
        public PDone expand(PCollection<Row> input) {
          BigQueryIO.Write<Row> write =
              BigQueryIO.<Row>write()
                  .useBeamSchema()
                  .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                  .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);

          final Boolean autoSharding = config.getBoolean("autoSharding");
          // use default value true for autoSharding if not configured for STORAGE_WRITE_API
          if (input.isBounded() == PCollection.IsBounded.UNBOUNDED) {
            write = write.withTriggeringFrequency(Duration.standardSeconds(5));
            if (autoSharding == null || autoSharding) {
              write = write.withAutoSharding();
            }
          }

          final Boolean useTestingBigQueryServices =
              config.getBoolean("useTestingBigQueryServices");
          if (useTestingBigQueryServices != null && useTestingBigQueryServices) {
            FakeBigQueryServices fbqs =
                new FakeBigQueryServices()
                    .withDatasetService(new FakeDatasetService())
                    .withJobService(new FakeJobService());
            write = write.withTestServices(fbqs);
          }

          String table = config.getString("table");
          if (table != null) {
            write = write.to(table);
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
