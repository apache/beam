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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithStorage;
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
      Set<String> toDrop =
          configuration.getDrop() == null
              ? Collections.emptySet()
              : new HashSet<>(configuration.getDrop());
      List<Integer> passThroughList = new ArrayList<>();
      if (append) {
        for (int i = 0; i < inputSchema.getFieldCount(); i++) {
          Schema.Field field = inputSchema.getField(i);
          if (!toDrop.contains(field.getName())) {
            passThroughList.add(i);
            outputSchemaBuilder = outputSchemaBuilder.addField(field);
          }
        }
      }
      int[] passThroughIndices = new int[passThroughList.size()];
      for (int i = 0; i < passThroughIndices.length; i++) {
        passThroughIndices[i] = passThroughList.get(i);
      }

      List<JavaRowUdf> udfs = new ArrayList<>(configuration.getFields().size());
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
      if (!handleErrors) {
        PCollection<Row> mapped =
            input
                .get(INPUT_ROWS_TAG)
                .apply(
                    "MapToFields",
                    ParDo.of(createSimpleDoFn(passThroughIndices, udfs, outputSchema)))
                .setRowSchema(outputSchema);
        return PCollectionRowTuple.of(OUTPUT_ROWS_TAG, mapped);
      }

      Schema errorSchema = ErrorHandling.errorSchema(inputSchema);
      PCollectionTuple pcolls =
          input
              .get(INPUT_ROWS_TAG)
              .apply(
                  "MapToFields",
                  ParDo.of(
                          createErrorHandlingDoFn(
                              passThroughIndices, udfs, outputSchema, errorSchema))
                      .withOutputTags(mappedValues, TupleTagList.of(errorValues)));
      pcolls.get(mappedValues).setRowSchema(outputSchema);
      pcolls.get(errorValues).setRowSchema(errorSchema);

      return PCollectionRowTuple.of(OUTPUT_ROWS_TAG, pcolls.get(mappedValues))
          .and(configuration.getErrorHandling().getOutput(), pcolls.get(errorValues));
    }

    private static final TupleTag<Row> mappedValues = new TupleTag<Row>() {};
    private static final TupleTag<Row> errorValues = new TupleTag<Row>() {};

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Function<Row, Object>[] resolveFunctions(List<JavaRowUdf> udfs) {
      Function<Row, Object>[] fns = new Function[udfs.size()];
      for (int i = 0; i < udfs.size(); i++) {
        try {
          fns[i] = udfs.get(i).getFunction();
        } catch (Exception exn) {
          throw new RuntimeException(exn);
        }
      }
      return fns;
    }

    private static boolean matchesExactScalarType(Schema.FieldType fieldType, Object val) {
      switch (fieldType.getTypeName()) {
        case STRING:
          return val instanceof String;
        case INT64:
          return val instanceof Long;
        case INT32:
          return val instanceof Integer;
        case DOUBLE:
          return val instanceof Double;
        case FLOAT:
          return val instanceof Float;
        case BOOLEAN:
          return val instanceof Boolean;
        case INT16:
          return val instanceof Short;
        case BYTE:
          return val instanceof Byte;
        case BYTES:
          return val instanceof byte[];
        case DECIMAL:
          return val instanceof java.math.BigDecimal;
        case DATETIME:
          return val instanceof org.joda.time.Instant;
        default:
          return false;
      }
    }

    private static Row mapRow(
        Row inputRow,
        int[] passThroughIndices,
        Function<Row, Object>[] fns,
        Schema.FieldType[] udfFieldTypes,
        Schema outputSchema) {
      int passLen = passThroughIndices.length;
      int fnLen = fns.length;
      Object[] values = new Object[passLen + fnLen];
      if (passLen > 0) {
        Object[] raw =
            inputRow instanceof RowWithStorage ? ((RowWithStorage) inputRow).getRawArray() : null;
        for (int i = 0; i < passLen; i++) {
          int idx = passThroughIndices[i];
          values[i] = raw != null ? raw[idx] : inputRow.getValue(idx);
        }
      }
      boolean needsValidation = false;
      for (int i = 0; i < fnLen; i++) {
        Object val = fns[i].apply(inputRow);
        Schema.FieldType ft = udfFieldTypes[i];
        if (val == null) {
          if (!ft.getNullable()) {
            throw new IllegalArgumentException(
                "Field " + outputSchema.getField(passLen + i).getName() + " is not nullable.");
          }
        } else if (!matchesExactScalarType(ft, val)) {
          needsValidation = true;
        }
        values[passLen + i] = val;
      }
      if (needsValidation) {
        return Row.withSchema(outputSchema).addValues(values).build();
      }
      return Row.attachValues(outputSchema, values);
    }

    private static Schema.FieldType[] extractUdfFieldTypes(int passLen, Schema outputSchema) {
      int fnLen = outputSchema.getFieldCount() - passLen;
      Schema.FieldType[] types = new Schema.FieldType[fnLen];
      for (int i = 0; i < fnLen; i++) {
        types[i] = outputSchema.getField(passLen + i).getType();
      }
      return types;
    }

    private static DoFn<Row, Row> createSimpleDoFn(
        int[] passThroughIndices, List<JavaRowUdf> udfs, Schema outputSchema) {
      Schema.FieldType[] udfFieldTypes =
          extractUdfFieldTypes(passThroughIndices.length, outputSchema);
      return new DoFn<Row, Row>() {
        private transient Function<Row, Object>[] fns;

        @Setup
        public void setup() {
          this.fns = resolveFunctions(udfs);
        }

        @ProcessElement
        public void processElement(@Element Row inputRow, OutputReceiver<Row> out) {
          Function<Row, Object>[] localFns = this.fns;
          if (localFns == null) {
            localFns = resolveFunctions(udfs);
            this.fns = localFns;
          }
          out.output(mapRow(inputRow, passThroughIndices, localFns, udfFieldTypes, outputSchema));
        }
      };
    }

    private static DoFn<Row, Row> createErrorHandlingDoFn(
        int[] passThroughIndices, List<JavaRowUdf> udfs, Schema outputSchema, Schema errorSchema) {
      Schema.FieldType[] udfFieldTypes =
          extractUdfFieldTypes(passThroughIndices.length, outputSchema);
      return new DoFn<Row, Row>() {
        private transient Function<Row, Object>[] fns;

        @Setup
        public void setup() {
          this.fns = resolveFunctions(udfs);
        }

        @ProcessElement
        public void processElement(@Element Row inputRow, MultiOutputReceiver out) {
          Row outputRow;
          try {
            Function<Row, Object>[] localFns = this.fns;
            if (localFns == null) {
              localFns = resolveFunctions(udfs);
              this.fns = localFns;
            }
            outputRow = mapRow(inputRow, passThroughIndices, localFns, udfFieldTypes, outputSchema);
          } catch (Exception exn) {
            out.get(errorValues).output(ErrorHandling.errorRecord(errorSchema, inputRow, exn));
            return;
          }
          out.get(mappedValues).output(outputRow);
        }
      };
    }
  }
}
