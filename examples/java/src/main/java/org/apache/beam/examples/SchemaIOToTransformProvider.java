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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

public class SchemaIOToTransformProvider implements SchemaTransformProvider {
  SchemaIOProvider provider;
  boolean isRead;

  public SchemaIOToTransformProvider(SchemaIOProvider provider, boolean isRead) {
    this.provider = provider;
    this.isRead = isRead;
  }

  @Override
  public String identifier() {
    return "transformer";
  }

  @Override
  public Schema configurationSchema() {
    Schema.Builder builder = Schema.builder();
    builder.addNullableField(
        "schema",
        Schema.FieldType.logicalType(new org.apache.beam.sdk.schemas.logicaltypes.Schema()));
    builder.addStringField("location");
    for (Field field : provider.configurationSchema().getFields()) {
      builder.addField(field);
    }
    return builder.build();
  }

  @Override
  public SchemaTransform from(Row configuration) {
    Row.Builder rowBuilder = Row.withSchema(provider.configurationSchema());
    int additionalFields =
        configurationSchema().getFieldCount() - provider.configurationSchema().getFieldCount();
    for (int i = 0; i < provider.configurationSchema().getFieldCount(); i++) {
      rowBuilder.addValue(configuration.getValue(i + additionalFields));
    }

    String location = configuration.getString("location");
    if (configuration.getSchemaValue("schema") == null && provider.requiresDataSchema()) {
      throw new IllegalArgumentException("No schema for IO that requires schema.");
    }
    SchemaIO schemaIO =
        provider.from(location, rowBuilder.build(), configuration.getSchemaValue("schema"));
    return isRead ? new SchemaIOToReadTransform(schemaIO) : new SchemaIOToWriteTransform(schemaIO);
  }

  @Override
  public List<String> getInputCollectionNames() {
    return isRead ? Arrays.asList() : Arrays.asList("input");
  }

  @Override
  public List<String> getOutputCollectionNames() {
    return isRead ? Arrays.asList("output") : Arrays.asList();
  }

  private static class SchemaIOToReadTransform implements SchemaTransform, Serializable {
    SchemaIO schemaIO;

    private SchemaIOToReadTransform(SchemaIO schemaIO) {
      this.schemaIO = schemaIO;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
          if (!input.getAll().isEmpty()) {
            throw new InvalidConfigurationException("");
          }
          return PCollectionRowTuple.of(
              "output",
              input.getPipeline().apply(schemaIO.buildReader()).setRowSchema(schemaIO.schema()));
        }
      };
    }
  }

  private static class SchemaIOToWriteTransform implements SchemaTransform, Serializable {
    SchemaIO schemaIO;

    private SchemaIOToWriteTransform(SchemaIO schemaIO) {
      this.schemaIO = schemaIO;
      // Verify that getSchema() matches input schema.
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
          // TODO: Verify that schema matches input schema.
          input.get("input").apply(schemaIO.buildWriter());
          return PCollectionRowTuple.empty(input.getPipeline());
        }
      };
    }
  }
}
