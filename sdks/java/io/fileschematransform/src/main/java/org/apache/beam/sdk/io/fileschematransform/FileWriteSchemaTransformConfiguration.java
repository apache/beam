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
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
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
    return new AutoValue_FileWriteSchemaTransformConfiguration_CsvConfiguration.Builder()
        .setCsvFormat(CSVFormat.DEFAULT.toString().toLowerCase());
  }

  public static ParquetConfiguration.Builder parquetConfigurationBuilder() {
    return new AutoValue_FileWriteSchemaTransformConfiguration_ParquetConfiguration.Builder();
  }

  public static XmlConfiguration.Builder xmlConfigurationBuilder() {
    return new AutoValue_FileWriteSchemaTransformConfiguration_XmlConfiguration.Builder();
  }

  /**
   * The format of the file content. Used as {@link String} key lookup of {@link
   * FileWriteSchemaTransformFormatProviders#loadProviders()}.
   */
  public abstract String getFormat();

  /** Specifies a common prefix to use for all generated filenames. */
  public abstract String getFilenamePrefix();

  /**
   * Specifies to compress all generated shard files by default, append the respective extension to
   * the filename. See {@link org.apache.beam.sdk.io.Compression} for expected values, though
   * stringified in all lowercase format.
   */
  @Nullable
  public abstract String getCompression();

  /** The number of output shards produced; a value of 1 disables sharding. */
  @Nullable
  public abstract Integer getNumShards();

  /** Uses the given {@link org.apache.beam.sdk.io.ShardNameTemplate} for naming output files. */
  @Nullable
  public abstract String getShardNameTemplate();

  /** Configures the filename suffix for written files. */
  @Nullable
  public abstract String getFilenameSuffix();

  /** Configures extra details related to writing CSV formatted files. */
  @Nullable
  public abstract CsvConfiguration getCsvConfiguration();

  /** Configures extra details related to writing Parquet formatted files. */
  @Nullable
  public abstract ParquetConfiguration getParquetConfiguration();

  /** Configures extra details related to writing XML formatted files. */
  @Nullable
  public abstract XmlConfiguration getXmlConfiguration();

  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * The format of the file content. Used as {@link String} key lookup of {@link
     * FileWriteSchemaTransformFormatProviders#loadProviders()}.
     */
    public abstract Builder setFormat(String value);

    /** Specifies a common prefix to use for all generated filenames. */
    public abstract Builder setFilenamePrefix(String value);

    /**
     * Specifies to compress all generated shard files by default, append the respective extension
     * to the filename. See {@link org.apache.beam.sdk.io.Compression} for expected values, though
     * stringified in all lowercase format.
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

    /**
     * Not to be confused with the CSV header, it is content written to the top of every sharded
     * file prior to the header. In the example below, all the text proceeding the header
     * 'column1,column2,column3' is the preamble.
     *
     * <p>Fake company, Inc. Lab experiment: abcdefg123456 Experiment date: 2022-12-05 Operator:
     * John Doe
     *
     * <p>column1,column2,column3 1,2,3 4,5,6
     */
    @Nullable
    public abstract String getPreamble();

    /**
     * The format of the written CSV file. See <a
     * href="https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html">org.apache.commons.csv.CSVFormat</a>
     * for allowed values, stringified in lowercase. Defaults to <a
     * href="https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html#DEFAULT">CSVFormat.DEFAULT</a>
     */
    public abstract String getCsvFormat();

    @AutoValue.Builder
    public abstract static class Builder {

      /**
       * Not to be confused with the CSV header, it is content written to the top of every sharded
       * file prior to the header. In the example below, all the text proceeding the header
       * 'column1,column2,column3' is the preamble.
       *
       * <p>Fake company, Inc. Lab experiment: abcdefg123456 Experiment date: 2022-12-05 Operator:
       * John Doe
       *
       * <p>column1,column2,column3 1,2,3 4,5,6
       */
      public abstract Builder setPreamble(String value);

      /**
       * The format of the written CSV file. See <a
       * href="https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html">org.apache.commons.csv.CSVFormat</a>
       * for allowed values, stringified in lowercase. Defaults to <a
       * href="https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html#DEFAULT">CSVFormat.DEFAULT</a>
       */
      public abstract Builder setCsvFormat(String value);

      public abstract CsvConfiguration build();
    }
  }

  /** Configures extra details related to writing Parquet formatted files. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class ParquetConfiguration {

    /**
     * Specifies compression codec. See org.apache.parquet.hadoop.metadata.CompressionCodecName for
     * allowed names, stringified in lowercase format.
     */
    public abstract String getCompressionCodecName();

    /** Specify row-group size; if not set or zero, a default is used by the underlying writer. */
    public abstract Integer getRowGroupSize();

    @AutoValue.Builder
    public abstract static class Builder {

      /**
       * Specifies compression codec. See org.apache.parquet.hadoop.metadata.CompressionCodecName
       * for allowed names, stringified in lowercase format.
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

    /** Sets the enclosing root element for the generated XML files. */
    public abstract String getRootElement();

    /**
     * The charset used to write the file. Defaults to {@link
     * java.nio.charset.StandardCharsets#UTF_8}.
     */
    public abstract String getCharset();

    @AutoValue.Builder
    public abstract static class Builder {

      /** Sets the enclosing root element for the generated XML files. */
      public abstract Builder setRootElement(String value);

      /**
       * The charset used to write the file. Defaults to {@link
       * java.nio.charset.StandardCharsets#UTF_8}.
       */
      public abstract Builder setCharset(String value);

      public abstract XmlConfiguration build();
    }
  }
}
