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
package org.apache.beam.sdk.schemas.transforms;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * A transform to add new nullable fields to a PCollection's schema. Elements are extended to have
 * the new schema, with null values used for the new fields. Any new fields added must be nullable.
 *
 * <p>Example use:
 *
 * <pre>{@code PCollection<Event> events = readEvents();
 * PCollection<Row> augmentedEvents =
 *   events.apply(AddFields.fields(Field.nullable("newField1", FieldType.STRING),
 *                                 Field.nullable("newField2", FieldType.INT64)));
 * }</pre>
 */
public class AddFields {
  /** Add all specified fields to the schema. */
  public static <T> Inner<T> fields(Field... fields) {
    return fields(Arrays.asList(fields));
  }

  /** Add all specified fields to the schema. */
  public static <T> Inner<T> fields(List<Field> fields) {
    for (Field field : fields) {
      if (!field.getType().getNullable()) {
        throw new IllegalArgumentException(
            "Only nullable fields can be added to an existing schema.");
      }
    }
    return new Inner<>(fields);
  }

  /** Inner PTransform for AddFields. */
  public static class Inner<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    private final List<Field> newFields;
    private final List<Object> nullValues;

    private Inner(List<Field> newFields) {
      this.newFields = newFields;
      this.nullValues = Collections.nCopies(newFields.size(), null);
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();
      Schema outputSchema =
          Schema.builder().addFields(inputSchema.getFields()).addFields(newFields).build();

      return input
          .apply(
              ParDo.of(
                  new DoFn<T, Row>() {
                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<Row> o) {
                      List<Object> values =
                          Lists.newArrayListWithCapacity(outputSchema.getFieldCount());
                      values.addAll(row.getValues());
                      values.addAll(nullValues);
                      Row newRow = Row.withSchema(outputSchema).attachValues(values).build();
                      o.output(newRow);
                    }
                  }))
          .setRowSchema(outputSchema);
    }
  }
}
