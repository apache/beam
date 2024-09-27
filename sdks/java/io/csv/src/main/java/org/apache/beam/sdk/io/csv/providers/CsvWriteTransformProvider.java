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
package org.apache.beam.sdk.io.csv.providers;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.csv.CsvIO;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for {@link CsvIO#write}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class CsvWriteTransformProvider
    extends TypedSchemaTransformProvider<CsvWriteTransformProvider.CsvWriteConfiguration> {
  private static final String INPUT_ROWS_TAG = "input";
  private static final String WRITE_RESULTS = "output";

  @Override
  protected Class<CsvWriteConfiguration> configurationClass() {
    return CsvWriteConfiguration.class;
  }

  @Override
  protected SchemaTransform from(CsvWriteConfiguration configuration) {
    return new CsvWriteTransform(configuration);
  }

  @Override
  public String identifier() {
    return String.format("beam:schematransform:org.apache.beam:csv_write:v1");
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_ROWS_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(WRITE_RESULTS);
  }

  /** Configuration for writing to BigQuery with Storage Write API. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class CsvWriteConfiguration {

    public void validate() {
      checkArgument(!Strings.isNullOrEmpty(getPath()), "Path for a CSV Write must be specified.");
      checkArgument(
          getDelimiter() == null || getDelimiter().length() == 1,
          "Only single-character delimiters supported, got '" + getDelimiter() + "'");
    }

    public static Builder builder() {
      return new AutoValue_CsvWriteTransformProvider_CsvWriteConfiguration.Builder();
    }

    @SchemaFieldDescription("The file path to write to.")
    public abstract String getPath();

    @SchemaFieldDescription("The field delimiter to use when writing records. Defaults to a comma.")
    public abstract @Nullable String getDelimiter();

    /** Builder for {@link CsvWriteConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setPath(String path);

      public abstract Builder setDelimiter(String delimiter);

      /** Builds a {@link CsvWriteConfiguration} instance. */
      public abstract CsvWriteConfiguration build();
    }
  }

  /** A {@link SchemaTransform} for {@link CsvIO#write}. */
  protected static class CsvWriteTransform extends SchemaTransform {

    private final CsvWriteConfiguration configuration;

    CsvWriteTransform(CsvWriteConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      CSVFormat format = CSVFormat.DEFAULT;
      if (configuration.getDelimiter() != null) {
        format = format.withDelimiter(configuration.getDelimiter().charAt(0));
      }

      // Preserve input windowing
      CsvIO.Write<Row> writeTransform =
          CsvIO.writeRows(configuration.getPath(), format).withSuffix("");
      if (input.get(INPUT_ROWS_TAG).isBounded() == PCollection.IsBounded.UNBOUNDED) {
        writeTransform = writeTransform.withWindowedWrites();
      }

      WriteFilesResult<?> result = input.get(INPUT_ROWS_TAG).apply(writeTransform);
      Schema outputSchema = Schema.of(Field.of("filename", FieldType.STRING));
      return PCollectionRowTuple.of(
          WRITE_RESULTS,
          result
              .getPerDestinationOutputFilenames()
              .apply(
                  "Collect filenames",
                  MapElements.into(TypeDescriptors.rows())
                      .via(
                          (destinationAndRow) ->
                              Row.withSchema(outputSchema)
                                  .withFieldValue("filename", destinationAndRow.getValue())
                                  .build()))
              .setRowSchema(outputSchema));
    }
  }
}
