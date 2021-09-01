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
package org.apache.beam.examples;

import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * An implementation of {@link SchemaTransformProvider} for reading and writing Avro files with
 * {@link AvroIO}.
 */
@Internal
@AutoService(SchemaTransformProvider.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class TextReadTransformProvider implements SchemaTransformProvider {
  /** Returns an id that uniquely represents this IO. */
  @Override
  public String identifier() {
    return "text:read:v1";
  }

  /**
   * Returns the expected schema of the configuration object. Note this is distinct from the schema
   * of the data source itself. No configuration expected for Avro.
   */
  @Override
  public Schema configurationSchema() {
    // TODO: Take input schema.
    return Schema.builder()
        .addStringField("location")
        .addNullableField(
            "schema",
            Schema.FieldType.logicalType(new org.apache.beam.sdk.schemas.logicaltypes.Schema()))
        .build();
  }

  @Override
  public List<String> getInputCollectionNames() {
    return Arrays.asList();
  }

  @Override
  public List<String> getOutputCollectionNames() {
    return Arrays.asList("output");
  }

  @Override
  public SchemaTransform from(Row configuration) {
    return new TextSchemaTransform(configuration);
  }

  /** An abstraction to create schema aware IOs. */
  private static class TextSchemaTransform implements SchemaTransform, Serializable {
    String location;
    Schema schema;

    private TextSchemaTransform(Row configuration) {
      location = configuration.getString("location");
      schema = configuration.getSchemaValue("schema");
      if (schema == null) schema = Schema.of(Field.of("single", FieldType.STRING));
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple begin) {
          if (!begin.getAll().isEmpty()) {
            throw new InvalidConfigurationException("");
          }
          PCollection<String> input =
              begin.getPipeline().apply("TextIORead", TextIO.read().from(location));
          PCollection<Row> rows;
          // TODO: Try to get Convert.toRows to work for this case.
          if (schema.getFieldCount() == 1
              && schema.getField(0).getType().equals(FieldType.STRING)) {
            // rows = input.setRowSchema(schema).apply(Convert.toRows());
            rows =
                input
                    .apply(
                        "convert",
                        MapElements.into(TypeDescriptors.rows())
                            .via((str) -> Row.withSchema(schema).addValue(str).build()))
                    .setRowSchema(schema);
          } else {
            rows = input.apply("convert", JsonToRow.withSchema(schema));
          }
          return PCollectionRowTuple.of("output", rows);
        }
      };
    }
  }
}
