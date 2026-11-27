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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
import org.apache.beam.sdk.values.RowWithStorage;

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
      Schema inputSchema = input.get(INPUT_ROWS_TAG).getSchema();
      Schema.Builder outputSchemaBuilder = new Schema.Builder();
      for (Schema.Field field : inputSchema.getFields()) {
        if (configuration.getFields().contains(field.getName())) {
          if (field.getType().getCollectionElementType() == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Exploded field %s must be an iterable type, got %s.",
                    field.getName(), field.getType()));
          } else {
            outputSchemaBuilder =
                outputSchemaBuilder.addField(
                    field.getName(), field.getType().getCollectionElementType());
          }
        } else {
          outputSchemaBuilder = outputSchemaBuilder.addField(field);
        }
      }
      Schema outputSchema = outputSchemaBuilder.build();

      PCollection<Row> result =
          input
              .get(INPUT_ROWS_TAG)
              .apply(
                  "Explode",
                  ParDo.of(
                      createDoFn(
                          configuration.getFields(),
                          configuration.getCrossProduct(),
                          outputSchema)));
      result.setRowSchema(outputSchema);

      return PCollectionRowTuple.of(OUTPUT_ROWS_TAG, result);
    }

    private static DoFn<Row, Row> createDoFn(
        List<String> fields, Boolean crossProductObj, Schema outputSchema) {
      boolean crossProduct;
      if (crossProductObj == null) {
        if (fields.size() > 1) {
          throw new IllegalArgumentException(
              "boolean cross product parameter required to explode more than one field");
        }
        crossProduct = false;
      } else {
        crossProduct = crossProductObj;
      }
      int numFields = outputSchema.getFieldCount();
      List<Integer> explodeList = new ArrayList<>(fields.size());
      List<Integer> copyList = new ArrayList<>(numFields);
      for (int i = 0; i < numFields; i++) {
        if (fields.contains(outputSchema.getField(i).getName())) {
          explodeList.add(i);
        } else {
          copyList.add(i);
        }
      }
      int[] explodeIndices = new int[explodeList.size()];
      for (int i = 0; i < explodeList.size(); i++) {
        explodeIndices[i] = explodeList.get(i);
      }
      int[] copyIndices = new int[copyList.size()];
      for (int i = 0; i < copyList.size(); i++) {
        copyIndices[i] = copyList.get(i);
      }

      if (explodeIndices.length == 1) {
        final int explodeIdx = explodeIndices[0];
        return new DoFn<Row, Row>() {
          @ProcessElement
          public void processElement(@Element Row inputRow, OutputReceiver<Row> out) {
            Iterable<Object> iterable = inputRow.getIterable(explodeIdx);
            if (iterable == null) {
              return;
            }
            if (iterable instanceof Collection && ((Collection<?>) iterable).isEmpty()) {
              return;
            }
            Iterator<Object> iter = iterable.iterator();
            if (!iter.hasNext()) {
              return;
            }
            Object[] template = new Object[numFields];
            Object[] raw =
                inputRow instanceof RowWithStorage
                    ? ((RowWithStorage) inputRow).getRawArray()
                    : null;
            for (int c : copyIndices) {
              template[c] = raw != null ? raw[c] : inputRow.getValue(c);
            }
            while (iter.hasNext()) {
              Object[] rowValues = template.clone();
              rowValues[explodeIdx] = iter.next();
              out.output(Row.attachValues(outputSchema, rowValues));
            }
          }
        };
      }

      if (crossProduct) {
        return new DoFn<Row, Row>() {
          @ProcessElement
          @SuppressWarnings("rawtypes")
          public void processElement(@Element Row inputRow, OutputReceiver<Row> out) {
            int k = explodeIndices.length;
            if (k == 0) {
              out.output(inputRow);
              return;
            }
            Iterable[] iterables = new Iterable[k];
            for (int i = 0; i < k; i++) {
              Iterable<Object> it = inputRow.getIterable(explodeIndices[i]);
              if (it == null) {
                return;
              }
              if (it instanceof Collection) {
                if (((Collection<?>) it).isEmpty()) {
                  return;
                }
              } else if (i > 0 && !it.iterator().hasNext()) {
                return;
              }
              iterables[i] = it;
            }
            Object[] current = new Object[numFields];
            Object[] raw =
                inputRow instanceof RowWithStorage
                    ? ((RowWithStorage) inputRow).getRawArray()
                    : null;
            for (int c : copyIndices) {
              current[c] = raw != null ? raw[c] : inputRow.getValue(c);
            }
            emitCrossProduct(iterables, 0, current, out);
          }

          @SuppressWarnings("rawtypes")
          private void emitCrossProduct(
              Iterable[] iterables, int pos, Object[] current, OutputReceiver<Row> out) {
            int fieldIdx = explodeIndices[pos];
            boolean atBottom = pos == explodeIndices.length - 1;
            for (Object value : iterables[pos]) {
              current[fieldIdx] = value;
              if (atBottom) {
                out.output(Row.attachValues(outputSchema, current.clone()));
              } else {
                emitCrossProduct(iterables, pos + 1, current, out);
              }
            }
          }
        };
      } else {
        return new DoFn<Row, Row>() {
          @ProcessElement
          @SuppressWarnings("rawtypes")
          public void processElement(@Element Row inputRow, OutputReceiver<Row> out) {
            int k = explodeIndices.length;
            if (k == 0) {
              return;
            }
            Iterator[] iterators = new Iterator[k];
            boolean anyNext = false;
            for (int i = 0; i < k; i++) {
              Iterable<Object> it = inputRow.getIterable(explodeIndices[i]);
              Iterator iter = it == null ? Collections.emptyIterator() : it.iterator();
              iterators[i] = iter;
              if (iter.hasNext()) {
                anyNext = true;
              }
            }
            if (!anyNext) {
              return;
            }
            Object[] template = new Object[numFields];
            Object[] raw =
                inputRow instanceof RowWithStorage
                    ? ((RowWithStorage) inputRow).getRawArray()
                    : null;
            for (int c : copyIndices) {
              template[c] = raw != null ? raw[c] : inputRow.getValue(c);
            }
            while (anyNext) {
              Object[] rowValues = template.clone();
              anyNext = false;
              for (int i = 0; i < k; i++) {
                Iterator iter = iterators[i];
                if (iter.hasNext()) {
                  rowValues[explodeIndices[i]] = iter.next();
                  if (iter.hasNext()) {
                    anyNext = true;
                  }
                } else {
                  int fieldIdx = explodeIndices[i];
                  if (!outputSchema.isFieldNullable(fieldIdx)) {
                    throw new IllegalArgumentException(
                        "Cannot set null on non-nullable field "
                            + outputSchema.getField(fieldIdx).getName());
                  }
                  rowValues[fieldIdx] = null;
                }
              }
              out.output(Row.attachValues(outputSchema, rowValues));
            }
          }
        };
      }
    }
  }
}
