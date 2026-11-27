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
import java.util.Collections;
import java.util.List;
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
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for Filter for the java language.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class JavaFilterTransformProvider
    extends TypedSchemaTransformProvider<JavaFilterTransformProvider.Configuration> {
  protected static final String INPUT_ROWS_TAG = "input";
  protected static final String OUTPUT_ROWS_TAG = "output";

  @Override
  protected Class<Configuration> configurationClass() {
    return Configuration.class;
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new JavaFilterTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:yaml:filter-java:v1";
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

    public abstract JavaRowUdf.Configuration getKeep();

    @Nullable
    public abstract ErrorHandling getErrorHandling();

    public static Builder builder() {
      return new AutoValue_JavaFilterTransformProvider_Configuration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setLanguage(String language);

      public abstract Builder setKeep(JavaRowUdf.Configuration keep);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract Configuration build();
    }
  }

  /** A {@link SchemaTransform} for Filter-java. */
  protected static class JavaFilterTransform extends SchemaTransform {

    private final Configuration configuration;

    JavaFilterTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Schema inputSchema = input.get(INPUT_ROWS_TAG).getSchema();
      JavaRowUdf keepFn;
      try {
        keepFn = new JavaRowUdf(this.configuration.getKeep(), inputSchema);
      } catch (MalformedURLException
          | ReflectiveOperationException
          | StringCompiler.CompileException exn) {
        throw new RuntimeException(exn);
      }
      if (!keepFn.getOutputType().withNullable(false).equals(Schema.FieldType.BOOLEAN)) {
        throw new RuntimeException(
            String.format(
                "KeepFn %s must return a boolean, but returns %s instead.",
                this.configuration.getKeep(), keepFn.getOutputType()));
      }
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());
      if (!handleErrors) {
        PCollection<Row> filtered =
            input
                .get(INPUT_ROWS_TAG)
                .apply("Filter", ParDo.of(createSimpleDoFn(keepFn)))
                .setRowSchema(inputSchema);
        return PCollectionRowTuple.of(OUTPUT_ROWS_TAG, filtered);
      }
      Schema errorSchema = ErrorHandling.errorSchema(inputSchema);
      PCollectionTuple pcolls =
          input
              .get(INPUT_ROWS_TAG)
              .apply(
                  "Filter",
                  ParDo.of(createErrorHandlingDoFn(keepFn, errorSchema))
                      .withOutputTags(filteredValues, TupleTagList.of(errorValues)));
      pcolls.get(filteredValues).setRowSchema(inputSchema);
      pcolls.get(errorValues).setRowSchema(errorSchema);

      return PCollectionRowTuple.of(OUTPUT_ROWS_TAG, pcolls.get(filteredValues))
          .and(configuration.getErrorHandling().getOutput(), pcolls.get(errorValues));
    }

    private static final TupleTag<Row> filteredValues = new TupleTag<Row>() {};
    private static final TupleTag<Row> errorValues = new TupleTag<Row>() {};

    private static Function<Row, Object> resolveKeepFn(JavaRowUdf keepFn) {
      try {
        return keepFn.getFunction();
      } catch (Exception exn) {
        throw new RuntimeException(exn);
      }
    }

    private static DoFn<Row, Row> createSimpleDoFn(JavaRowUdf keepFn) {
      return new DoFn<Row, Row>() {
        private transient Function<Row, Object> fn;

        @Setup
        public void setup() {
          this.fn = resolveKeepFn(keepFn);
        }

        @ProcessElement
        public void processElement(@Element Row inputRow, OutputReceiver<Row> out) {
          Function<Row, Object> localFn = this.fn;
          if (localFn == null) {
            localFn = resolveKeepFn(keepFn);
            this.fn = localFn;
          }
          if ((boolean) localFn.apply(inputRow)) {
            out.output(inputRow);
          }
        }
      };
    }

    private static DoFn<Row, Row> createErrorHandlingDoFn(JavaRowUdf keepFn, Schema errorSchema) {
      return new DoFn<Row, Row>() {
        private transient Function<Row, Object> fn;

        @Setup
        public void setup() {
          this.fn = resolveKeepFn(keepFn);
        }

        @ProcessElement
        public void processElement(@Element Row inputRow, MultiOutputReceiver out) {
          boolean keep = false;
          try {
            Function<Row, Object> localFn = this.fn;
            if (localFn == null) {
              localFn = resolveKeepFn(keepFn);
              this.fn = localFn;
            }
            keep = (boolean) localFn.apply(inputRow);
          } catch (Exception exn) {
            out.get(errorValues).output(ErrorHandling.errorRecord(errorSchema, inputRow, exn));
            return;
          }
          if (keep) {
            out.get(filteredValues).output(inputRow);
          }
        }
      };
    }
  }
}
