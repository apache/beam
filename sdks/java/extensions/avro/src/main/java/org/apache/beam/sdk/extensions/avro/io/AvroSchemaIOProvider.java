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
package org.apache.beam.sdk.extensions.avro.io;

import com.google.auto.service.AutoService;
import java.io.Serializable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * An implementation of {@link SchemaIOProvider} for reading and writing Avro files with {@link
 * AvroIO}.
 */
@Internal
@AutoService(SchemaIOProvider.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class AvroSchemaIOProvider implements SchemaIOProvider {
  /** Returns an id that uniquely represents this IO. */
  @Override
  public String identifier() {
    return "avro";
  }

  /**
   * Returns the expected schema of the configuration object. Note this is distinct from the schema
   * of the data source itself. No configuration expected for Avro.
   */
  @Override
  public Schema configurationSchema() {
    return Schema.builder().addNullableField("writeWindowSizeSeconds", FieldType.INT64).build();
  }

  /**
   * Produce a SchemaIO given a String representing the data's location, the schema of the data that
   * resides there, and some IO-specific configuration object.
   */
  @Override
  public AvroSchemaIO from(String location, Row configuration, Schema dataSchema) {
    return new AvroSchemaIO(location, dataSchema, configuration);
  }

  @Override
  public boolean requiresDataSchema() {
    return true;
  }

  @Override
  public IsBounded isBounded() {
    // This supports streaming now as well but there's no option for this. The move to
    // SchemaTransform will remove the need to provide this.
    return IsBounded.BOUNDED;
  }

  /** An abstraction to create schema aware IOs. */
  private static class AvroSchemaIO implements SchemaIO, Serializable {
    protected final Schema dataSchema;
    protected final String location;
    protected final @Nullable Duration windowSize;

    private AvroSchemaIO(String location, Schema dataSchema, Row configuration) {
      this.dataSchema = dataSchema;
      this.location = location;
      if (configuration.getInt64("writeWindowSizeSeconds") != null) {
        windowSize = Duration.standardSeconds(configuration.getInt64("writeWindowSizeSeconds"));
      } else {
        windowSize = null;
      }
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
          return begin
              .apply(
                  "AvroIORead",
                  AvroIO.readGenericRecords(AvroUtils.toAvroSchema(dataSchema, null, null))
                      .withBeamSchemas(true)
                      .from(location))
              .apply("ToRows", Convert.toRows());
        }
      };
    }

    @Override
    public PTransform<PCollection<Row>, POutput> buildWriter() {
      return new PTransform<PCollection<Row>, POutput>() {
        @Override
        public PDone expand(PCollection<Row> input) {
          PCollection<GenericRecord> asRecords =
              input.apply("ToGenericRecords", Convert.to(GenericRecord.class));
          AvroIO.Write<GenericRecord> avroWrite =
              AvroIO.writeGenericRecords(AvroUtils.toAvroSchema(dataSchema, null, null))
                  .to(location);
          if (input.isBounded() == IsBounded.UNBOUNDED || windowSize != null) {
            asRecords =
                asRecords.apply(
                    Window.into(
                        FixedWindows.of(
                            windowSize == null ? Duration.standardMinutes(1) : windowSize)));
            avroWrite = avroWrite.withWindowedWrites().withNumShards(1);
          } else {
            avroWrite = avroWrite.withoutSharding();
          }
          return asRecords.apply("AvroIOWrite", avroWrite);
        }
      };
    }
  }
}
