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

import com.google.auto.value.AutoValue;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.commons.csv.CSVFormat;

/**
 * The configuration for building file writing transforms using {@link
 * org.apache.beam.sdk.schemas.transforms.SchemaTransform} and {@link
 * org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider}.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class FileWriteSchemaTransformConfiguration {

  public static FileWriteSchemaTransformConfiguration.Builder builder() {
    return new AutoValue_FileWriteSchemaTransformConfiguration.Builder();
  }

  public static CsvConfiguration.Builder csvConfigurationBuilder() {
    return new AutoValue_FileWriteSchemaTransformConfiguration_CsvConfiguration.Builder();
  }

  public static ParquetConfiguration.Builder parquetConfigurationBuilder() {
    return new AutoValue_FileWriteSchemaTransformConfiguration_ParquetConfiguration.Builder();
  }

  public static XmlConfiguration.Builder xmlConfigurationBuilder() {
    return new AutoValue_FileWriteSchemaTransformConfiguration_XmlConfiguration.Builder()
        .setCharset(StandardCharsets.UTF_8.name());
  }

  @SchemaFieldDescription(
      "The format of the file content. Value must be one of: \"avro\", \"csv\", \"json\", \"parquet\", \"xml\"")
  public abstract String getFormat();

  @SchemaFieldDescription("A common prefix to use for all generated filenames.")
  public abstract String getFilenamePrefix();

  /** See {@link org.apache.beam.sdk.io.Compression} for expected values. */
  @SchemaFieldDescription(
      "The compression of all generated shard files. By default, appends the respective extension to the filename. Valid options can be found in: https://beam.apache.org/releases/javadoc/2.46.0/org/apache/beam/sdk/io/Compression.html")
  @Nullable
  public abstract String getCompression();

  @SchemaFieldDescription("The number of output shards produced; a value of 1 disables sharding.")
  @Nullable
  public abstract Integer getNumShards();

  /** See {@link org.apache.beam.sdk.io.ShardNameTemplate} for the expected values. */
  @SchemaFieldDescription("Uses the given shard name template for naming output files.")
  @Nullable
  public abstract String getShardNameTemplate();

  @SchemaFieldDescription("Configures the filename suffix for written files.")
  @Nullable
  public abstract String getFilenameSuffix();

  @SchemaFieldDescription("Configures extra details related to writing CSV formatted files.")
  @Nullable
  public abstract CsvConfiguration getCsvConfiguration();

  @SchemaFieldDescription("Configures extra details related to writing Parquet formatted files.")
  @Nullable
  public abstract ParquetConfiguration getParquetConfiguration();

  @SchemaFieldDescription("Configures extra details related to writing XML formatted files.")
  @Nullable
  public abstract XmlConfiguration getXmlConfiguration();

  abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    /** The format of the file content. See {@link #getFormat()} for more details. */
    public abstract Builder setFormat(String value);

    /** A common prefix to use for all generated filenames. */
    public abstract Builder setFilenamePrefix(String value);

    /**
     * The file {@link org.apache.beam.sdk.io.Compression} See {@link #getCompression()} for more
     * details.
     */
    public abstract Builder setCompression(String value);

    /** The number of output shards produced; a value of 1 disables sharding. */
    public abstract Builder setNumShards(Integer value);

    /** Uses the given {@link org.apache.beam.sdk.io.ShardNameTemplate} for naming output files. */
    public abstract Builder setShardNameTemplate(String value);

    /** Configures the filename suffix for written files. */
    public abstract Builder setFilenameSuffix(String value);

    /** Configures extra details related to writing CSV formatted files. */
    public abstract Builder setCsvConfiguration(CsvConfiguration value);

    /** Configures extra details related to writing Parquet formatted files. */
    public abstract Builder setParquetConfiguration(ParquetConfiguration value);

    /** Configures extra details related to writing XML formatted files. */
    public abstract Builder setXmlConfiguration(XmlConfiguration value);

    public abstract FileWriteSchemaTransformConfiguration build();
  }

  /** Configures extra details related to writing CSV formatted files. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class CsvConfiguration {

    /** See {@link CSVFormat.Predefined#values()} for a list of allowed values. */
    @SchemaFieldDescription(
        "The Predefined format of the written CSV file. Valid options can be found in: https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html")
    public abstract String getPredefinedCsvFormat();

    @AutoValue.Builder
    public abstract static class Builder {

      /**
       * The {@link CSVFormat.Predefined#name()} of the written CSV file. See {@link
       * CSVFormat.Predefined#values()} for a list of allowed values.
       */
      public abstract Builder setPredefinedCsvFormat(String value);

      public abstract CsvConfiguration build();
    }
  }

  /** Configures extra details related to writing Parquet formatted files. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class ParquetConfiguration {

    @SchemaFieldDescription(
        "Specifies compression codec. Valid values are: \"GZIP\", \"LZO\", \"SNAPPY\", \"UNCOMPRESSED\"")
    public abstract String getCompressionCodecName();

    @SchemaFieldDescription(
        "Specify row-group size; if not set or zero, a default is used by the underlying writer.")
    @Nullable
    public abstract Integer getRowGroupSize();

    @AutoValue.Builder
    public abstract static class Builder {

      /**
       * Specifies compression codec. See org.apache.parquet.hadoop.metadata.CompressionCodecName
       * for allowed names.
       */
      public abstract Builder setCompressionCodecName(String value);

      /** Specify row-group size; if not set or zero, a default is used by the underlying writer. */
      public abstract Builder setRowGroupSize(Integer value);

      public abstract ParquetConfiguration build();
    }
  }

  /** Configures extra details related to writing XML formatted files. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class XmlConfiguration {

    @SchemaFieldDescription("Sets the enclosing root element for the generated XML files.")
    public abstract String getRootElement();

    @SchemaFieldDescription("The charset used to write the file. Defaults to UTF_8")
    public abstract String getCharset();

    @AutoValue.Builder
    public abstract static class Builder {

      /** Sets the enclosing root element for the generated XML files. */
      public abstract Builder setRootElement(String value);

      /**
       * The charset used to write the file. Defaults to {@link
       * java.nio.charset.StandardCharsets#UTF_8}'s {@link Charset#name()}.
       */
      public abstract Builder setCharset(String value);

      public abstract XmlConfiguration build();
    }
  }
}
