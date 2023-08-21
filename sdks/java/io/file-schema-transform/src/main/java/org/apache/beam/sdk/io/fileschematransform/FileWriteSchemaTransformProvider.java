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
package org.apache.beam.sdk.io.fileschematransform;

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.AVRO;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.CSV;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.PARQUET;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.XML;
import static org.apache.beam.sdk.values.TypeDescriptors.rows;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/**
 * A {@link TypedSchemaTransformProvider} implementation for writing a {@link Row} {@link
 * PCollection} to file systems, driven by a {@link FileWriteSchemaTransformConfiguration}.
 */
@AutoService(SchemaTransformProvider.class)
public class FileWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<FileWriteSchemaTransformConfiguration> {

  public static final Field FILE_NAME_FIELD = Field.of("fileName", FieldType.STRING);
  public static final Schema OUTPUT_SCHEMA = Schema.of(FILE_NAME_FIELD);
  public static final Schema ERROR_SCHEMA =
      Schema.builder().addStringField("error").addNullableByteArrayField("row").build();

  private static final String IDENTIFIER = "beam:schematransform:org.apache.beam:file_write:v1";
  static final String INPUT_TAG = "input";
  static final String OUTPUT_TAG = "output";
  static final String ERROR_STRING = "error";
  static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  static final TupleTag<String> RESULT_TAG = new TupleTag<String>() {};

  /** Provides the required {@link TypedSchemaTransformProvider#configurationClass()}. */
  @Override
  protected Class<FileWriteSchemaTransformConfiguration> configurationClass() {
    return FileWriteSchemaTransformConfiguration.class;
  }

  /** Builds a {@link SchemaTransform} from a {@link FileWriteSchemaTransformConfiguration}. */
  @Override
  protected SchemaTransform from(FileWriteSchemaTransformConfiguration configuration) {
    return new FileWriteSchemaTransform(configuration);
  }

  /** Returns the {@link TypedSchemaTransformProvider#identifier()} required for registration. */
  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  /** The expected {@link PCollectionRowTuple} input tags. */
  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  /** The expected {@link PCollectionRowTuple} output tags. */
  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  /**
   * A {@link PTransform} that converts a {@link PCollectionRowTuple} of {@link
   * #inputCollectionNames()} tagged {@link Row}s into a {@link PCollectionRowTuple} of {@link
   * #outputCollectionNames()} tagged {@link Row}s.
   */
  static class FileWriteSchemaTransform extends SchemaTransform {

    final FileWriteSchemaTransformConfiguration configuration;

    FileWriteSchemaTransform(FileWriteSchemaTransformConfiguration configuration) {
      validateConfiguration(configuration);
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      if (input.getAll().isEmpty() || input.getAll().size() > 1) {
        throw new IllegalArgumentException(
            String.format(
                "%s expects a single %s tagged PCollection<Row> input",
                FileWriteSchemaTransform.class.getName(), INPUT_TAG));
      }

      PCollection<Row> rowInput = input.get(INPUT_TAG);

      PTransform<PCollection<Row>, PCollectionTuple> transform =
          getProvider().buildTransform(configuration, rowInput.getSchema());

      PCollectionTuple files = rowInput.apply("Write Rows", transform);
      PCollection<Row> output =
          files
              .get(RESULT_TAG)
              .apply(
                  "Filenames to Rows",
                  MapElements.into(rows())
                      .via(
                          (String name) ->
                              Row.withSchema(OUTPUT_SCHEMA)
                                  .withFieldValue(FILE_NAME_FIELD.getName(), name)
                                  .build()))
              .setRowSchema(OUTPUT_SCHEMA);

      if (files.has(ERROR_TAG)) {
        return PCollectionRowTuple.of(OUTPUT_TAG, output).and(ERROR_STRING, files.get(ERROR_TAG));
      } else {
        return PCollectionRowTuple.of(OUTPUT_TAG, output);
      }
    }

    /**
     * A helper method to retrieve the mapped {@link FileWriteSchemaTransformFormatProvider} from a
     * {@link FileWriteSchemaTransformConfiguration#getFormat()}.
     */
    FileWriteSchemaTransformFormatProvider getProvider() {
      Map<String, FileWriteSchemaTransformFormatProvider> providers =
          FileWriteSchemaTransformFormatProviders.loadProviders();
      if (!providers.containsKey(configuration.getFormat())) {
        throw new IllegalArgumentException(
            String.format(
                "%s is not a supported format. See %s for a list of supported formats.",
                configuration.getFormat(),
                FileWriteSchemaTransformFormatProviders.class.getName()));
      }
      // resolves [dereference.of.nullable]
      Optional<FileWriteSchemaTransformFormatProvider> provider =
          Optional.ofNullable(providers.get(configuration.getFormat()));
      checkState(provider.isPresent());
      return provider.get();
    }

    /**
     * Validates a {@link FileWriteSchemaTransformConfiguration} for correctness depending on its
     * {@link FileWriteSchemaTransformConfiguration#getFormat()}.
     */
    static void validateConfiguration(FileWriteSchemaTransformConfiguration configuration) {
      String format = configuration.getFormat();
      if (configuration.getCsvConfiguration() != null && !format.equals(CSV)) {
        throw new IllegalArgumentException(
            String.format(
                "configuration with %s is not compatible with a %s format",
                FileWriteSchemaTransformConfiguration.CsvConfiguration.class.getName(), format));
      }
      if (configuration.getParquetConfiguration() != null && !format.equals(PARQUET)) {
        throw new IllegalArgumentException(
            String.format(
                "configuration with %s is not compatible with a %s format",
                FileWriteSchemaTransformConfiguration.ParquetConfiguration.class.getName(),
                format));
      }
      if (configuration.getXmlConfiguration() != null && !format.equals(XML)) {
        throw new IllegalArgumentException(
            String.format(
                "configuration with %s is not compatible with a %s format",
                FileWriteSchemaTransformConfiguration.XmlConfiguration.class.getName(), format));
      }
      if (format.equals(AVRO) && !Strings.isNullOrEmpty(configuration.getCompression())) {
        throw new IllegalArgumentException(
            "configuration with compression is not compatible with AvroIO");
      }
      if (format.equals(PARQUET) && !Strings.isNullOrEmpty(configuration.getCompression())) {
        throw new IllegalArgumentException(
            "configuration with compression is not compatible with ParquetIO");
      }
    }
  }
}
