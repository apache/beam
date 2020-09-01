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
package org.apache.beam.sdk.io.parquet;

import com.google.auto.service.AutoService;
import java.io.Serializable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * An implementation of {@link SchemaIOProvider} for reading and writing parquet files with {@link
 * ParquetIO}.
 */
@Internal
@AutoService(SchemaIOProvider.class)
public class ParquetSchemaIOProvider implements SchemaIOProvider {
  /** Returns an id that uniquely represents this IO. */
  @Override
  public String identifier() {
    return "parquet";
  }

  /**
   * Returns the expected schema of the configuration object. Note this is distinct from the schema
   * of the data source itself. No configuration expected for parquet.
   */
  @Override
  public Schema configurationSchema() {
    return Schema.builder().build();
  }

  /**
   * Produce a SchemaIO given a String representing the data's location, the schema of the data that
   * resides there, and some IO-specific configuration object.
   */
  @Override
  public ParquetSchemaIO from(String location, Row configuration, Schema dataSchema) {
    return new ParquetSchemaIO(location, dataSchema);
  }

  @Override
  public boolean requiresDataSchema() {
    return true;
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  /** An abstraction to create schema aware IOs. */
  private static class ParquetSchemaIO implements SchemaIO, Serializable {
    protected final Schema dataSchema;
    protected final String location;

    private ParquetSchemaIO(String location, Schema dataSchema) {
      this.dataSchema = dataSchema;
      this.location = location;
    }

    @Override
    public Schema schema() {
      return dataSchema;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildReader() {
      return new PTransform<PBegin, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PBegin begin) {
          PTransform<PCollection<GenericRecord>, PCollection<Row>> readConverter =
              GenericRecordReadConverter.builder().beamSchema(dataSchema).build();

          return begin
              .apply(
                  "ParquetIORead",
                  ParquetIO.read(AvroUtils.toAvroSchema(dataSchema)).from(location))
              .apply("GenericRecordToRow", readConverter);
        }
      };
    }

    @Override
    public PTransform<PCollection<Row>, POutput> buildWriter() {
      throw new UnsupportedOperationException("Writing to a Parquet file is not supported");
    }
  }
}
