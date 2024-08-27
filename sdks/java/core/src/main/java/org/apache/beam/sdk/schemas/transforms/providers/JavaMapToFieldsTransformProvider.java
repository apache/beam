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
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for MapToFields for the java language.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class JavaMapToFieldsTransformProvider
    extends TypedSchemaTransformProvider<JavaMapToFieldsTransformProvider.Configuration> {
  protected static final String INPUT_ROWS_TAG = "input";
  protected static final String OUTPUT_ROWS_TAG = "output";

  @Override
  protected Class<Configuration> configurationClass() {
    return Configuration.class;
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new JavaMapToFieldsTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:yaml:map_to_fields-java:v1";
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
    public abstract String getLanguage();

    @Nullable
    public abstract Boolean getAppend();

    @Nullable
    public abstract List<String> getDrop();

    public abstract Map<String, JavaRowUdf.Configuration> getFields();

    @Nullable
    public abstract ErrorHandling getErrorHandling();

    public static Builder builder() {
      return new AutoValue_JavaMapToFieldsTransformProvider_Configuration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setLanguage(String language);

      public abstract Builder setAppend(Boolean append);

      public abstract Builder setDrop(List<String> drop);

      public abstract Builder setFields(Map<String, JavaRowUdf.Configuration> fields);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract Configuration build();
    }
  }

  /** A {@link SchemaTransform} for MapToFields-java. */
  protected static class JavaMapToFieldsTransform extends SchemaTransform {

    private final Configuration configuration;

    JavaMapToFieldsTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Schema inputSchema = input.get(INPUT_ROWS_TAG).getSchema();
      Schema.Builder outputSchemaBuilder = new Schema.Builder();
      // TODO(yaml): Consider allowing the full java schema naming syntax
      // (perhaps as a different dialect/language).
      boolean append = configuration.getAppend() != null && configuration.getAppend();
      List<String> toDrop =
          configuration.getDrop() == null ? Collections.emptyList() : configuration.getDrop();
      List<JavaRowUdf> udfs = new ArrayList<>();
      if (append) {
        for (Schema.Field field : inputSchema.getFields()) {
          if (!toDrop.contains(field.getName())) {
            try {
              udfs.add(
                  new JavaRowUdf(
                      JavaRowUdf.Configuration.builder().setExpression(field.getName()).build(),
                      inputSchema));
            } catch (MalformedURLException
                | ReflectiveOperationException
                | StringCompiler.CompileException exn) {
              throw new RuntimeException(exn);
            }
            outputSchemaBuilder = outputSchemaBuilder.addField(field);
          }
        }
      }
      for (Map.Entry<String, JavaRowUdf.Configuration> entry :
          configuration.getFields().entrySet()) {
        try {
          JavaRowUdf udf = new JavaRowUdf(entry.getValue(), inputSchema);
          udfs.add(udf);
          outputSchemaBuilder = outputSchemaBuilder.addField(entry.getKey(), udf.getOutputType());
        } catch (MalformedURLException
            | ReflectiveOperationException
            | StringCompiler.CompileException exn) {
          throw new RuntimeException(exn);
        }
      }
      Schema outputSchema = outputSchemaBuilder.build();
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());
      Schema errorSchema = ErrorHandling.errorSchema(inputSchema);

      PCollectionTuple pcolls =
          input
              .get(INPUT_ROWS_TAG)
              .apply(
                  "MapToFields",
                  ParDo.of(createDoFn(udfs, outputSchema, errorSchema, handleErrors))
                      .withOutputTags(mappedValues, TupleTagList.of(errorValues)));
      pcolls.get(mappedValues).setRowSchema(outputSchema);
      pcolls.get(errorValues).setRowSchema(errorSchema);

      PCollectionRowTuple result =
          PCollectionRowTuple.of(OUTPUT_ROWS_TAG, pcolls.get(mappedValues));
      if (handleErrors) {
        result = result.and(configuration.getErrorHandling().getOutput(), pcolls.get(errorValues));
      }
      return result;
    }

    private static final TupleTag<Row> mappedValues = new TupleTag<Row>() {};
    private static final TupleTag<Row> errorValues = new TupleTag<Row>() {};

    private static DoFn<Row, Row> createDoFn(
        List<JavaRowUdf> udfs, Schema outputSchema, Schema errorSchema, boolean handleErrors) {
      return new DoFn<Row, Row>() {
        @ProcessElement
        public void processElement(@Element Row inputRow, MultiOutputReceiver out) {
          Row outputRow;
          try {
            Row.Builder builder = Row.withSchema(outputSchema);
            for (JavaRowUdf udf : udfs) {
              builder.addValue(udf.getFunction().apply(inputRow));
            }
            outputRow = builder.build();
          } catch (Exception exn) {
            if (handleErrors) {
              out.get(errorValues).output(ErrorHandling.errorRecord(errorSchema, inputRow, exn));
              outputRow = null;
            } else {
              throw new RuntimeException(exn);
            }
          }
          if (outputRow != null) {
            out.get(mappedValues).output(outputRow);
          }
        }
      };
    }
  }
}
