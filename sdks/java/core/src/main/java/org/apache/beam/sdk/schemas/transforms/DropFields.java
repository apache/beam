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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/**
 * A transform to drop fields from a schema.
 *
 * <p>This transform acts as the inverse of the {@link Select} transform. A list of fields to drop
 * is specified, and all fields in the schema that are not specified are selected. For example:
 *
 * <pre>{@code @DefaultSchema(JavaFieldSchema.class)
 * public class UserEvent {
 *   public String userId;
 *   public String eventId;
 *   public int eventType;
 *   public Location location;
 * }}</pre>
 *
 * <pre>{@code @DefaultSchema(JavaFieldSchema.class)
 * public class Location {
 *   public double latitude;
 *   public double longtitude;
 * }
 *
 * PCollection<UserEvent> events = readUserEvents();
 * // Drop the location field.
 * PCollection<Row> noLocation = events.apply(DropFields.fields("location"));
 * // Drop the latitude field.
 * PCollection<Row> noLatitude = events.apply(DropFields.fields("location.latitude"));
 * }</pre>
 */
@Experimental(Kind.SCHEMAS)
public class DropFields {
  public static <T> Inner<T> fields(String... fields) {
    return fields(FieldAccessDescriptor.withFieldNames(fields));
  }

  public static <T> Inner<T> fields(Integer... fieldIds) {
    return fields(FieldAccessDescriptor.withFieldIds(fieldIds));
  }

  public static <T> Inner<T> fields(FieldAccessDescriptor fieldsToDrop) {
    return new Inner<>(fieldsToDrop);
  }

  /** Implementation class for DropFields. */
  public static class Inner<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    private final FieldAccessDescriptor fieldsToDrop;

    private Inner(FieldAccessDescriptor fieldsToDrop) {
      this.fieldsToDrop = fieldsToDrop;
    }

    FieldAccessDescriptor complement(Schema inputSchema, FieldAccessDescriptor input) {
      // Create a FieldAccessDescriptor that select all fields _not_ selected in the input
      // descriptor. Maintain the original order of the schema.
      List<String> fieldNamesToSelect = newArrayList();
      Map<FieldAccessDescriptor.FieldDescriptor, FieldAccessDescriptor> nestedFieldsToSelect =
          Maps.newHashMap();
      for (int i = 0; i < inputSchema.getFieldCount(); ++i) {
        if (input.fieldIdsAccessed().contains(i)) {
          // This field is selected, so exclude it from the complement.
          continue;
        }
        Field field = inputSchema.getField(i);
        Map<Integer, FieldAccessDescriptor.FieldDescriptor> nestedFields =
            input.getNestedFieldsAccessed().keySet().stream()
                .collect(Collectors.toMap(k -> k.getFieldId(), k -> k));

        FieldAccessDescriptor.FieldDescriptor fieldDescriptor = nestedFields.get(i);
        if (fieldDescriptor != null) {
          // Some subfields are selected, so recursively calculate the complementary subfields to
          // select.
          FieldType fieldType = inputSchema.getField(i).getType();
          for (FieldAccessDescriptor.FieldDescriptor.Qualifier qualifier :
              fieldDescriptor.getQualifiers()) {
            switch (qualifier.getKind()) {
              case LIST:
                fieldType = fieldType.getCollectionElementType();
                break;
              case MAP:
                fieldType = fieldType.getMapValueType();
                break;
              default:
                throw new RuntimeException("Unexpected field descriptor type.");
            }
          }
          checkArgument(fieldType.getTypeName().isCompositeType());
          FieldAccessDescriptor nestedDescriptor =
              input.getNestedFieldsAccessed().get(fieldDescriptor);
          nestedFieldsToSelect.put(
              fieldDescriptor, complement(fieldType.getRowSchema(), nestedDescriptor));
        } else {
          // Neither the field nor the subfield is selected. This means we should select it.
          fieldNamesToSelect.add(field.getName());
        }
      }

      FieldAccessDescriptor fieldAccess = FieldAccessDescriptor.withFieldNames(fieldNamesToSelect);
      for (Map.Entry<FieldAccessDescriptor.FieldDescriptor, FieldAccessDescriptor> entry :
          nestedFieldsToSelect.entrySet()) {
        fieldAccess = fieldAccess.withNestedField(entry.getKey(), entry.getValue());
      }
      return fieldAccess.resolve(inputSchema);
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();
      FieldAccessDescriptor selectDescriptor =
          complement(inputSchema, fieldsToDrop.resolve(inputSchema));

      return input.apply(Select.fieldAccess(selectDescriptor));
    }
  }
}
