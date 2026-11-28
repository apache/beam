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
package org.apache.beam.sdk.schemas.transforms.providers;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Booleans;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for Explode.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class JavaExplodeTransformProvider
    extends TypedSchemaTransformProvider<JavaExplodeTransformProvider.Configuration> {
  protected static final String INPUT_ROWS_TAG = "input";
  protected static final String OUTPUT_ROWS_TAG = "output";

  @Override
  protected Class<Configuration> configurationClass() {
    return Configuration.class;
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new ExplodeTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:yaml:explode:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_ROWS_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_ROWS_TAG);
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    @Nullable
    public abstract List<String> getFields();

    @Nullable
    public abstract Boolean getCrossProduct();

    public static Builder builder() {
      return new AutoValue_JavaExplodeTransformProvider_Configuration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setFields(List<String> fields);

      public abstract Builder setCrossProduct(@Nullable Boolean append);

      public abstract Configuration build();
    }
  }

  /** A {@link SchemaTransform} for Explode. */
  protected static class ExplodeTransform extends SchemaTransform {

    private final Configuration configuration;

    ExplodeTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      List<String> fields = configuration.getFields();
      boolean crossProduct = resolveCrossProduct(fields, configuration.getCrossProduct());
      Schema inputSchema = input.get(INPUT_ROWS_TAG).getSchema();
      Schema.Builder outputSchemaBuilder = new Schema.Builder();
      for (Schema.Field field : inputSchema.getFields()) {
        if (fields.contains(field.getName())) {
          Schema.FieldType elementType = field.getType().getCollectionElementType();
          if (elementType == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Exploded field %s must be an iterable type, got %s.",
                    field.getName(), field.getType()));
          } else {
            if (!crossProduct && fields.size() > 1) {
              elementType = elementType.withNullable(true);
            }
            outputSchemaBuilder = outputSchemaBuilder.addField(field.getName(), elementType);
          }
        } else {
          outputSchemaBuilder = outputSchemaBuilder.addField(field);
        }
      }
      Schema outputSchema = outputSchemaBuilder.build();

      PCollection<Row> result =
          input
              .get(INPUT_ROWS_TAG)
              .apply("Explode", ParDo.of(createDoFn(fields, crossProduct, outputSchema)));
      result.setRowSchema(outputSchema);

      return PCollectionRowTuple.of(OUTPUT_ROWS_TAG, result);
    }

    private static boolean resolveCrossProduct(
        List<String> fields, @Nullable Boolean crossProductObj) {
      if (crossProductObj == null) {
        if (fields.size() > 1) {
          throw new IllegalArgumentException(
              "boolean cross product parameter required to explode more than one field");
        }
        return false;
      }
      return crossProductObj;
    }

    private static DoFn<Row, Row> createDoFn(
        List<String> fields, boolean crossProduct, Schema outputSchema) {
      int numFields = outputSchema.getFields().size();
      boolean[] toExplode =
          Booleans.toArray(
              IntStream.range(0, numFields)
                  .mapToObj(index -> fields.contains(outputSchema.getField(index).getName()))
                  .collect(Collectors.toList()));
      if (crossProduct) {
        return new DoFn<Row, Row>() {
          @ProcessElement
          public void processElement(@Element Row inputRow, OutputReceiver<Row> out) {
            emitCrossProduct(inputRow, 0, new Object[numFields], out);
          }

          private void emitCrossProduct(
              Row inputRow, int index, Object[] current, OutputReceiver<Row> out) {
            if (index == numFields) {
              out.output(Row.withSchema(outputSchema).addValues(Arrays.asList(current)).build());
            } else if (toExplode[index]) {
              Iterable<Object> values = inputRow.getIterable(index);
              if (values != null) {
                for (Object value : values) {
                  current[index] = value;
                  emitCrossProduct(inputRow, index + 1, current, out);
                }
              }
            } else {
              current[index] = inputRow.getValue(index);
              emitCrossProduct(inputRow, index + 1, current, out);
            }
          }
        };
      } else {
        return new DoFn<Row, Row>() {
          @ProcessElement
          public void processElement(@Element Row inputRow, OutputReceiver<Row> out) {
            @SuppressWarnings("rawtypes")
            Iterator[] iterators = new Iterator[numFields];
            for (int i = 0; i < numFields; i++) {
              if (toExplode[i]) {
                Iterable<Object> values = inputRow.getIterable(i);
                iterators[i] = values == null ? Collections.emptyIterator() : values.iterator();
              }
            }
            while (IntStream.range(0, numFields)
                .anyMatch(index -> toExplode[index] && iterators[index].hasNext())) {
              Row.Builder builder = Row.withSchema(outputSchema);
              for (int i = 0; i < numFields; i++) {
                if (toExplode[i]) {
                  if (iterators[i].hasNext()) {
                    builder.addValue(iterators[i].next());
                  } else {
                    builder.addValue(null);
                  }
                } else {
                  builder.addValue(inputRow.getValue(i));
                }
              }
              out.output(builder.build());
            }
          }
        };
      }
    }
  }
}
