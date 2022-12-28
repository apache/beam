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

import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration.ParquetConfiguration;
import org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration.XmlConfiguration;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.Providers;
import org.apache.beam.sdk.schemas.io.payloads.JsonPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * {@link FileWriteSchemaTransformFormatProviders} contains {@link
 * FileWriteSchemaTransformFormatProvider} implementations.
 *
 * <p>The design goals of this class are to enable clean {@link
 * FileWriteSchemaTransformConfiguration#getFormat()} lookups that map to the appropriate {@link
 * org.apache.beam.sdk.io.FileIO.Write} that encodes the file data into the configured format.
 */
@Internal
public final class FileWriteSchemaTransformFormatProviders {
  static final String AVRO = "avro";
  static final String CSV = "csv";
  static final String JSON = "json";
  static final String PARQUET = "parquet";
  static final String XML = "xml";

  /** Load all {@link FileWriteSchemaTransformFormatProvider} implementations. */
  public static Map<String, FileWriteSchemaTransformFormatProvider> loadProviders() {
    return Providers.loadProviders(FileWriteSchemaTransformFormatProvider.class);
  }

  /** A {@link FileWriteSchemaTransformFormatProvider} for avro format. */
  @AutoService(FileWriteSchemaTransformFormatProvider.class)
  public static class Avro implements FileWriteSchemaTransformFormatProvider {
    @Override
    public String identifier() {
      return AVRO;
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildTransform(
        FileWriteSchemaTransformConfiguration configuration, Schema schema) {
      return new PTransform<PCollection<Row>, PDone>() {
        @Override
        public PDone expand(PCollection<Row> input) {

          org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
          AvroGenericCoder coder = AvroGenericCoder.of(avroSchema);

          PCollection<GenericRecord> avro =
              input
                  .apply("Row To Avro Generic Record", mapRowsToGenericRecords(schema))
                  .setCoder(coder);

          AvroIO.Write<GenericRecord> write =
              AvroIO.writeGenericRecords(avroSchema).to(configuration.getFilenamePrefix());

          if (configuration.getNumShards() != null) {
            write = write.withNumShards(getNumShards(configuration));
          }

          if (configuration.getShardNameTemplate() != null) {
            write = write.withShardNameTemplate(getShardNameTemplate(configuration));
          }

          avro.apply("Write Avro", write);

          return PDone.in(input.getPipeline());
        }
      };
    }
  }

  /** A {@link FileWriteSchemaTransformFormatProvider} for CSV format. */
  @AutoService(FileWriteSchemaTransformFormatProvider.class)
  public static class Csv implements FileWriteSchemaTransformFormatProvider {
    @Override
    public String identifier() {
      return CSV;
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildTransform(
        FileWriteSchemaTransformConfiguration configuration, Schema schema) {
      // TODO(https://github.com/apache/beam/issues/24469)
      throw new UnsupportedOperationException();
    }
  }

  /** A {@link FileWriteSchemaTransformFormatProvider} for JSON format. */
  @AutoService(FileWriteSchemaTransformFormatProvider.class)
  public static class Json implements FileWriteSchemaTransformFormatProvider {
    final String suffix = String.format(".%s", JSON);

    @Override
    public String identifier() {
      return JSON;
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildTransform(
        FileWriteSchemaTransformConfiguration configuration, Schema schema) {
      return new PTransform<PCollection<Row>, PDone>() {
        @Override
        public PDone expand(PCollection<Row> input) {

          PCollection<String> json = input.apply("Row To Json", mapRowsToJsonStrings(schema));

          TextIO.Write write =
              TextIO.write().to(configuration.getFilenamePrefix()).withSuffix(suffix);

          if (configuration.getCompression() != null) {
            write = write.withCompression(getCompression(configuration));
          }

          if (configuration.getFilenameSuffix() != null) {
            write = write.withSuffix(getSuffix(configuration));
          }

          if (configuration.getShardNameTemplate() != null) {
            write = write.withShardNameTemplate(getShardNameTemplate(configuration));
          }

          if (configuration.getNumShards() != null) {
            write = write.withNumShards(getNumShards(configuration));
          }

          json.apply("Write Json", write);

          return PDone.in(input.getPipeline());
        }
      };
    }

    /** Builds a {@link MapElements} transform to map {@link Row} to JSON strings. */
    MapElements<Row, String> mapRowsToJsonStrings(Schema schema) {
      return MapElements.into(strings()).via(new RowToJsonFn(schema));
    }

    private static class RowToJsonFn implements SerializableFunction<Row, String> {
      private final PayloadSerializer payloadSerializer;

      RowToJsonFn(Schema schema) {
        payloadSerializer =
            new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());
      }

      @Override
      public String apply(Row input) {
        return new String(payloadSerializer.serialize(input), StandardCharsets.UTF_8);
      }
    }
  }

  /** A {@link FileWriteSchemaTransformFormatProvider} for Parquet format. */
  @AutoService(FileWriteSchemaTransformFormatProvider.class)
  public static class Parquet implements FileWriteSchemaTransformFormatProvider {
    final String suffix = String.format(".%s", PARQUET);

    @Override
    public String identifier() {
      return PARQUET;
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildTransform(
        FileWriteSchemaTransformConfiguration configuration, Schema schema) {
      return new PTransform<PCollection<Row>, PDone>() {
        @Override
        public PDone expand(PCollection<Row> input) {
          org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
          AvroGenericCoder coder = AvroGenericCoder.of(avroSchema);

          input
              .apply("Row To GenericRecord", mapRowsToGenericRecords(schema))
              .setCoder(coder)
              .apply(
                  "Write Parquet",
                  FileIO.<GenericRecord>write()
                      .via(buildSink(parquetConfiguration(configuration), schema))
                      .to(configuration.getFilenamePrefix())
                      .withSuffix(suffix));

          return PDone.in(input.getPipeline());
        }
      };
    }

    private ParquetIO.Sink buildSink(ParquetConfiguration configuration, Schema schema) {

      ParquetIO.Sink sink =
          ParquetIO.sink(AvroUtils.toAvroSchema(schema))
              .withCompressionCodec(
                  CompressionCodecName.valueOf(configuration.getCompressionCodecName()));

      if (configuration.getRowGroupSize() != null) {
        sink = sink.withRowGroupSize(getRowGroupSize(configuration));
      }

      return sink;
    }

    private static ParquetConfiguration parquetConfiguration(
        FileWriteSchemaTransformConfiguration configuration) {
      // resolves Checker Framework incompatible argument for requireNonNull parameter
      Optional<ParquetConfiguration> parquetConfiguration =
          Optional.ofNullable(configuration.getParquetConfiguration());
      checkState(parquetConfiguration.isPresent());
      return parquetConfiguration.get();
    }

    private static Integer getRowGroupSize(ParquetConfiguration configuration) {
      // resolves Checker Framework [unboxing.of.nullable] unboxing a possibly-null reference
      Optional<Integer> rowGroupSize = Optional.ofNullable(configuration.getRowGroupSize());
      checkState(rowGroupSize.isPresent());
      return rowGroupSize.get();
    }
  }

  /** A {@link FileWriteSchemaTransformFormatProvider} for XML format. */
  @AutoService(FileWriteSchemaTransformFormatProvider.class)
  public static class Xml implements FileWriteSchemaTransformFormatProvider {

    @Override
    public String identifier() {
      return XML;
    }

    @Override
    public PTransform<PCollection<Row>, PDone> buildTransform(
        FileWriteSchemaTransformConfiguration configuration, Schema schema) {
      return new PTransform<PCollection<Row>, PDone>() {
        @Override
        public PDone expand(PCollection<Row> input) {

          PCollection<XmlRowAdapter> xml =
              input.apply(
                  "Row to XML",
                  MapElements.into(TypeDescriptor.of(XmlRowAdapter.class)).via(new RowToXmlFn()));

          XmlConfiguration xmlConfig = xmlConfiguration(configuration);

          Charset charset = Charset.forName(xmlConfig.getCharset());

          xml.apply(
              "Write XML",
              XmlIO.<XmlRowAdapter>write()
                  .to(configuration.getFilenamePrefix())
                  .withCharset(charset)
                  .withRootElement(xmlConfig.getRootElement())
                  .withRecordClass(XmlRowAdapter.class));

          return PDone.in(input.getPipeline());
        }
      };
    }

    static class RowToXmlFn implements SerializableFunction<Row, XmlRowAdapter> {

      @Override
      public XmlRowAdapter apply(Row input) {
        XmlRowAdapter result = new XmlRowAdapter();
        result.wrapRow(input);
        return result;
      }
    }

    private static XmlConfiguration xmlConfiguration(
        FileWriteSchemaTransformConfiguration configuration) {
      // resolves Checker Framework incompatible argument for parameter of requireNonNull
      Optional<XmlConfiguration> result = Optional.ofNullable(configuration.getXmlConfiguration());
      checkState(result.isPresent());
      return result.get();
    }
  }

  /** Builds a {@link MapElements}i transform to map {@link Row}s to {@link GenericRecord}s. */
  static MapElements<Row, GenericRecord> mapRowsToGenericRecords(Schema beamSchema) {
    return MapElements.into(TypeDescriptor.of(GenericRecord.class))
        .via(AvroUtils.getRowToGenericRecordFunction(AvroUtils.toAvroSchema(beamSchema)));
  }

  private static Compression getCompression(FileWriteSchemaTransformConfiguration configuration) {
    // resolves Checker Framework incompatible argument for valueOf parameter
    Optional<String> compression = Optional.ofNullable(configuration.getCompression());
    checkState(compression.isPresent());
    return Compression.valueOf(compression.get());
  }

  private static String getSuffix(FileWriteSchemaTransformConfiguration configuration) {
    // resolves Checker Framework incompatible argument for parameter suffix of withSuffix
    Optional<String> suffix = Optional.ofNullable(configuration.getFilenameSuffix());
    checkState(suffix.isPresent());
    return suffix.get();
  }

  private static Integer getNumShards(FileWriteSchemaTransformConfiguration configuration) {
    // resolves Checker Framework unboxing a possibly-null reference
    Optional<Integer> numShards = Optional.ofNullable(configuration.getNumShards());
    checkState(numShards.isPresent());
    return numShards.get();
  }

  private static String getShardNameTemplate(FileWriteSchemaTransformConfiguration configuration) {
    // resolves Checker Framework incompatible null argument for parameter shardTemplate
    Optional<String> shardNameTemplate = Optional.ofNullable(configuration.getShardNameTemplate());
    checkState(shardNameTemplate.isPresent());
    return shardNameTemplate.get();
  }
}
