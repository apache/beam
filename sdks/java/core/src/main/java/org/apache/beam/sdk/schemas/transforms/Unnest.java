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

import com.google.auto.value.AutoValue;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

/**
 * A {@link PTransform} to unnest nested rows.
 *
 * <p>For example, consider a Row with the following nestedschema:
 *
 * <p>UserEvent Schema: userid: INT64 timestamp: DATETIME location: LatLong
 *
 * <p>LatLong Schema: latitude: DOUBLE longitude: DOUBLE
 *
 * <p>After unnesting, all of the rows will be converted to rows satisfying the following schema:
 *
 * <p>UserEvent Schema: userid: INT64 timestamp: DATETIME location.latitude: DOUBLE
 * location.longitude: DOUBLE
 *
 * <p>By default nested names are concatenated to generated the unnested name, however {@link
 * Unnest.Inner#withFieldNameFunction} can be used to specify a custom naming policy.
 *
 * <p>Note that currently array and map values are not unnested.
 */
@Experimental(Kind.SCHEMAS)
public class Unnest {
  public static <T> Inner<T> create() {
    return new AutoValue_Unnest_Inner.Builder<T>().setFieldNameFunction(CONCAT_FIELD_NAMES).build();
  }
  /**
   * This is the default naming policy for naming fields. Every field name in the path to a given
   * field is concated with _ characters.
   */
  public static final SerializableFunction<List<String>, String> CONCAT_FIELD_NAMES =
      l -> {
        return String.join("_", l);
      };
  /**
   * This policy keeps the raw nested field name. If two differently-nested fields have the same
   * name, unnesting will fail with this policy.
   */
  public static final SerializableFunction<List<String>, String> KEEP_NESTED_NAME =
      l -> {
        return l.get(l.size() - 1);
      };
  /** Returns the result of unnesting the given schema. The default naming policy is used. */
  static Schema getUnnestedSchema(Schema schema) {
    List<String> nameComponents = Lists.newArrayList();
    return getUnnestedSchema(schema, nameComponents, CONCAT_FIELD_NAMES);
  }
  /** Returns the result of unnesting the given schema with the given naming policy. */
  static Schema getUnnestedSchema(Schema schema, SerializableFunction<List<String>, String> fn) {
    List<String> nameComponents = Lists.newArrayList();
    return getUnnestedSchema(schema, nameComponents, fn);
  }

  private static Schema getUnnestedSchema(
      Schema schema, List<String> nameComponents, SerializableFunction<List<String>, String> fn) {
    Schema.Builder builder = Schema.builder();
    for (Field field : schema.getFields()) {
      nameComponents.add(field.getName());
      if (field.getType().getTypeName().isCompositeType()) {
        Schema nestedSchema = getUnnestedSchema(field.getType().getRowSchema(), nameComponents, fn);
        for (Field nestedField : nestedSchema.getFields()) {
          builder.addField(nestedField);
        }
      } else {
        String name = fn.apply(nameComponents);
        Field newField = field.toBuilder().setName(name).build();
        builder.addField(newField);
      }
      nameComponents.remove(nameComponents.size() - 1);
    }
    return builder.build();
  }
  /** Unnest a row. */
  static Row unnestRow(Row input, Schema unnestedSchema) {
    Row.Builder builder = Row.withSchema(unnestedSchema);
    unnestRow(input, builder);
    return builder.build();
  }

  private static void unnestRow(Row input, Row.Builder output) {
    for (int i = 0; i < input.getSchema().getFieldCount(); ++i) {
      Field field = input.getSchema().getField(i);
      if (field.getType().getTypeName().isCompositeType()) {
        unnestRow(input.getRow(i), output);
      } else {
        output.addValue(input.getValue(i));
      }
    }
  }
  /** A {@link PTransform} that unnests nested row. */
  @AutoValue
  public abstract static class Inner<T> extends PTransform<PCollection<T>, PCollection<Row>> {
    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setFieldNameFunction(SerializableFunction<List<String>, String> fn);

      abstract Inner<T> build();
    };

    abstract SerializableFunction<List<String>, String> getFieldNameFunction();
    /**
     * Sets a policy for naming deeply-nested fields.
     *
     * <p>This is needed to prevent name collisions when differently-nested fields have the same
     * name. The default is to use the {@link #CONCAT_FIELD_NAMES} strategy that concatenates all
     * names in the path to generate the unnested name. For example, an unnested name might be
     * field1_field2_field3. In some cases the {@link #KEEP_NESTED_NAME} strategy can be used to
     * keep only the most-deeply nested name. However if this results in conflicting names (e.g. if
     * a schema has two subrows that each have the same schema this will happen), the pipeline will
     * fail at construction time.
     *
     * <p>An example of using this function to customize the separator character:
     *
     * <pre>{@code
     * pc.apply(Unnest.<Type>create().withFieldNameFunction(l -> Strings.join("+", l)));
     * }</pre>
     */
    public Inner<T> withFieldNameFunction(SerializableFunction<List<String>, String> fn) {
      return toBuilder().setFieldNameFunction(fn).build();
    }

    @Override
    public PCollection<Row> expand(PCollection<T> input) {
      Schema inputSchema = input.getSchema();
      Schema outputSchema = getUnnestedSchema(inputSchema, getFieldNameFunction());
      return input
          .apply(
              ParDo.of(
                  new DoFn<T, Row>() {
                    @ProcessElement
                    public void processElement(@Element Row row, OutputReceiver<Row> o) {
                      o.output(unnestRow(row, outputSchema));
                    }
                  }))
          .setRowSchema(outputSchema);
    }
  }
}
