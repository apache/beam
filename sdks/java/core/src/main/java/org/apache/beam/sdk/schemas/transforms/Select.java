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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.ListQualifier;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.MapQualifier;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.Qualifier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Maps;

/**
 * A {@link PTransform} for selecting a subset of fields from a schema type.
 *
 * <p>This transforms allows projecting out a subset of fields from a schema type. The output of
 * this transform is of type {@link Row}, though that can be converted into any other type with
 * matching schema using the {@link Convert} transform.
 *
 * <p>For example, consider the following POJO type:
 *
 * <pre>{@code
 * {@literal @}DefaultSchema(JavaFieldSchema.class)
 * public class UserEvent {
 *   public String userId;
 *   public String eventId;
 *   public int eventType;
 *   public Location location;
 * }
 *
 * {@literal @}DefaultSchema(JavaFieldSchema.class)
 * public class Location {
 *   public double latitude;
 *   public double longtitude;
 * }
 * }</pre>
 *
 * Say you want to select just the set of userId, eventId pairs from each element, you would write
 * the following:
 *
 * <pre>{@code
 * PCollection<UserEvent> events = readUserEvents();
 * PCollection<Row> rows = event.apply(Select.fieldNames("userId", "eventId"));
 * }</pre>
 *
 * It's possible to select a nested field as well. For example, if you want just the location
 * information from each element:
 *
 * <pre>{@code
 * PCollection<UserEvent> events = readUserEvents();
 * PCollection<Row> rows = event.apply(Select.fieldNames("location.*"))
 * }</pre>
 */
@Experimental(Kind.SCHEMAS)
public class Select<T> extends PTransform<PCollection<T>, PCollection<Row>> {
  private final FieldAccessDescriptor fieldAccessDescriptor;

  private Select(FieldAccessDescriptor fieldAccessDescriptor) {
    this.fieldAccessDescriptor = fieldAccessDescriptor;
  }

  /** Select a set of top-level field ids from the row. */
  public static <T> Select<T> fieldIds(Integer... ids) {
    return new Select<>(FieldAccessDescriptor.withFieldIds(ids));
  }

  /** Select a set of top-level field names from the row. */
  public static <T> Select<T> fieldNames(String... names) {
    return new Select<>(FieldAccessDescriptor.withFieldNames(names));
  }

  /**
   * Select a set of fields described in a {@link FieldAccessDescriptor}.
   *
   * <p>This allows for nested fields to be selected as well.
   */
  public static <T> Select<T> fieldAccess(FieldAccessDescriptor fieldAccessDescriptor) {
    return new Select<>(fieldAccessDescriptor);
  }

  @Override
  public PCollection<Row> expand(PCollection<T> input) {
    Schema inputSchema = input.getSchema();
    FieldAccessDescriptor resolved = fieldAccessDescriptor.resolve(inputSchema);
    Schema outputSchema = getOutputSchema(inputSchema, resolved);

    return input
        .apply(
            ParDo.of(
                new DoFn<T, Row>() {
                  // TODO: This should be the same as resolved so that Beam knows which fields
                  // are being accessed. Currently Beam only supports wildcard descriptors.
                  // Once BEAM-4457 is fixed, fix this.
                  @FieldAccess("selectFields")
                  final FieldAccessDescriptor fieldAccessDescriptor =
                      FieldAccessDescriptor.withAllFields();

                  @ProcessElement
                  public void process(@FieldAccess("selectFields") Row row, OutputReceiver<Row> r) {
                    r.output(selectRow(row, resolved, inputSchema, outputSchema));
                  }
                }))
        .setRowSchema(outputSchema);
  }

  // Currently we don't flatten selected nested fields.
  static Schema getOutputSchema(Schema inputSchema, FieldAccessDescriptor fieldAccessDescriptor) {
    if (fieldAccessDescriptor.getAllFields()) {
      return inputSchema;
    }
    Schema.Builder builder = new Schema.Builder();
    for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
      builder.addField(inputSchema.getField(fieldId));
    }

    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor fieldDescriptor = nested.getKey();
      Field field = inputSchema.getField(checkNotNull(fieldDescriptor.getFieldId()));
      FieldType outputType =
          getOutputSchemaHelper(
              field.getType(), nested.getValue(), fieldDescriptor.getQualifiers(), 0);
      builder.addField(field.getName(), outputType);
    }
    return builder.build();
  }

  private static FieldType getOutputSchemaHelper(
      FieldType inputFieldType,
      FieldAccessDescriptor fieldAccessDescriptor,
      List<Qualifier> qualifiers,
      int qualifierPosition) {
    if (qualifierPosition >= qualifiers.size()) {
      // We have walked through any containers, and are at a row type. Extract the subschema
      // for the row, preserving nullable attributes.
      checkArgument(inputFieldType.getTypeName().isCompositeType());
      return FieldType.row(getOutputSchema(inputFieldType.getRowSchema(), fieldAccessDescriptor))
          .withNullable(inputFieldType.getNullable());
    }

    Qualifier qualifier = qualifiers.get(qualifierPosition);
    switch (qualifier.getKind()) {
      case LIST:
        checkArgument(qualifier.getList().equals(ListQualifier.ALL));
        FieldType componentType = checkNotNull(inputFieldType.getCollectionElementType());
        FieldType outputComponent =
            getOutputSchemaHelper(
                    componentType, fieldAccessDescriptor, qualifiers, qualifierPosition + 1)
                .withNullable(componentType.getNullable());
        return FieldType.array(outputComponent).withNullable(inputFieldType.getNullable());
      case MAP:
        checkArgument(qualifier.getMap().equals(MapQualifier.ALL));
        FieldType keyType = checkNotNull(inputFieldType.getMapKeyType());
        FieldType valueType = checkNotNull(inputFieldType.getMapValueType());
        FieldType outputValueType =
            getOutputSchemaHelper(
                    valueType, fieldAccessDescriptor, qualifiers, qualifierPosition + 1)
                .withNullable(valueType.getNullable());
        return FieldType.map(keyType, outputValueType).withNullable(inputFieldType.getNullable());
      default:
        throw new RuntimeException("unexpected");
    }
  }

  static Row selectRow(
      Row input,
      FieldAccessDescriptor fieldAccessDescriptor,
      Schema inputSchema,
      Schema outputSchema) {
    if (fieldAccessDescriptor.getAllFields()) {
      return input;
    }

    Row.Builder output = Row.withSchema(outputSchema);
    for (int fieldId : fieldAccessDescriptor.fieldIdsAccessed()) {
      // TODO: Once we support specific qualifiers (like array slices), extract them here.
      output.addValue(input.getValue(fieldId));
    }

    for (Map.Entry<FieldDescriptor, FieldAccessDescriptor> nested :
        fieldAccessDescriptor.getNestedFieldsAccessed().entrySet()) {
      FieldDescriptor field = nested.getKey();
      String fieldName = inputSchema.nameOf(checkNotNull(field.getFieldId()));
      FieldType nestedInputType = inputSchema.getField(field.getFieldId()).getType();
      FieldType nestedOutputType = outputSchema.getField(fieldName).getType();
      Object value =
          selectRowHelper(
              field.getQualifiers(),
              0,
              input.getValue(fieldName),
              nested.getValue(),
              nestedInputType,
              nestedOutputType);
      output.addValue(value);
    }
    return output.build();
  }

  @SuppressWarnings("unchecked")
  private static Object selectRowHelper(
      List<Qualifier> qualifiers,
      int qualifierPosition,
      Object value,
      FieldAccessDescriptor fieldAccessDescriptor,
      FieldType inputType,
      FieldType outputType) {
    if (qualifierPosition >= qualifiers.size()) {
      Row row = (Row) value;
      return selectRow(
          row, fieldAccessDescriptor, inputType.getRowSchema(), outputType.getRowSchema());
    }

    if (fieldAccessDescriptor.getAllFields()) {
      // Since we are selecting all fields (and we do not yet support array slicing), short circuit.
      return value;
    }

    Qualifier qualifier = qualifiers.get(qualifierPosition);
    switch (qualifier.getKind()) {
      case LIST:
        {
          FieldType nestedInputType = checkNotNull(inputType.getCollectionElementType());
          FieldType nestedOutputType = checkNotNull(outputType.getCollectionElementType());
          List<Object> list = (List) value;
          List selectedList = Lists.newArrayListWithCapacity(list.size());
          for (Object o : list) {
            Object selected =
                selectRowHelper(
                    qualifiers,
                    qualifierPosition + 1,
                    o,
                    fieldAccessDescriptor,
                    nestedInputType,
                    nestedOutputType);
            selectedList.add(selected);
          }
          return selectedList;
        }
      case MAP:
        {
          FieldType nestedInputType = checkNotNull(inputType.getMapValueType());
          FieldType nestedOutputType = checkNotNull(outputType.getMapValueType());
          Map<Object, Object> map = (Map) value;
          Map selectedMap = Maps.newHashMapWithExpectedSize(map.size());
          for (Map.Entry<Object, Object> entry : map.entrySet()) {
            Object selected =
                selectRowHelper(
                    qualifiers,
                    qualifierPosition + 1,
                    entry.getValue(),
                    fieldAccessDescriptor,
                    nestedInputType,
                    nestedOutputType);
            selectedMap.put(entry.getKey(), selected);
          }
          return selectedMap;
        }
      default:
        throw new RuntimeException("Unexpected type " + qualifier.getKind());
    }
  }
}
