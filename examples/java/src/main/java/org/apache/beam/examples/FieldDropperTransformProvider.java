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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

@AutoService(SchemaTransformProvider.class)
public class FieldDropperTransformProvider implements SchemaTransformProvider {

  @Override
  public String identifier() {
    return "fieldDropper";
  }

  @Override
  public Schema configurationSchema() {
    Field field = Field.of("droppedField", FieldType.STRING).withDescription("A description");
    return Schema.of(field);
  }

  @Override
  public SchemaTransform from(Row configuration) {
    return new FieldDropperSchemaTransform(configuration);
  }

  @Override
  public List<String> getInputCollectionNames() {
    return Arrays.asList("input");
  }

  @Override
  public List<String> getOutputCollectionNames() {
    return Arrays.asList("output");
  }

  static class FieldDropperSchemaTransform implements SchemaTransform, Serializable {
    // Field number of inputSchema to drop.
    String droppedField;

    public FieldDropperSchemaTransform(Row configuration) {
      droppedField = configuration.getString(0);
    }

    public static class DropDoFn implements SerializableFunction<Row, Row> {

      private int fieldIndexInInputSchema;
      private Schema outputSchema;

      public DropDoFn(Schema inputSchema, String droppedField) {
        Schema.Builder schemaBuilder = Schema.builder();
        for (int i = 0; i < inputSchema.getFieldCount(); i++) {
          Field field = inputSchema.getField(i);
          if (field.getName().equals(droppedField)) {
            fieldIndexInInputSchema = i;
          } else {
            schemaBuilder.addField(field);
          }
          // Throw exception if index is -1;
        }
        outputSchema = schemaBuilder.build();
      }

      public Schema getOutputSchema() {
        return outputSchema;
      }

      @Override
      public Row apply(Row row) {
        Row.Builder builder = Row.withSchema(outputSchema);
        for (int i = 0; i < row.getFieldCount(); i++) {
          if (i != fieldIndexInInputSchema) {
            builder.addValue(row.getValue(i));
          }
        }
        return builder.build();
      }
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
          PCollection<Row> rows = input.get("input");
          DropDoFn fn = new DropDoFn(rows.getSchema(), droppedField);
          PCollection<Row> result =
              rows.apply("drop field", MapElements.into(TypeDescriptors.rows()).via(fn))
                  .setRowSchema(fn.getOutputSchema());
          return PCollectionRowTuple.of("output", result);
        }
      };
    }
  }
}
