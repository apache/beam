/*
 * Copyright 2017 Kochava, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.aws.redshift;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.DelegateCoder.CodingFunction;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;

/**
 * {@link PTransform PTransforms} for reading and writing Redshift tables.
 */
public class Redshift {

  private Redshift() {
  }

  /**
   * A POJO describing a {@link DataSource}, either providing directly a {@link DataSource} or all
   * properties allowing to create a {@link DataSource}.
   */
  @AutoValue
  public abstract static class DataSourceConfiguration implements Serializable {

    /**
     * Creates a {@link DataSourceConfiguration} from the given parameters, making the parameters
     * easy to gather from flags, etc. Uses the Amazon JDBC driver included in this module.
     */
    public static DataSourceConfiguration create(
        String endpoint,
        int port,
        String database,
        String user,
        String password) {
      checkArgument(0 < port && port < 65536, "port");
      return new AutoValue_Redshift_DataSourceConfiguration.Builder()
          .setEndpoint(checkNotNull(endpoint))
          .setPort(port)
          .setDatabase(checkNotNull(database))
          .setUser(checkNotNull(user))
          .setPassword(checkNotNull(password))
          .build();
    }

    /**
     * Creates a {@link DataSourceConfiguration} from a {@link DataSource}, useful for proving an
     * alternative to the bundled Amazon Redshift JDBC driver.
     */
    public static DataSourceConfiguration create(DataSource dataSource) {
      return new AutoValue_Redshift_DataSourceConfiguration.Builder()
          .setDataSource(checkNotNull(dataSource)).build();
    }

    /**
     * Constructs a {@link DataSource} from this config.
     */
    DataSource buildDataSource() {
      if (getDataSource() != null) {
        return getDataSource();
      }
      com.amazon.redshift.jdbc.DataSource dataSource = new com.amazon.redshift.jdbc.DataSource();
      dataSource.setURL(getUrl());
      dataSource.setUserID(getUser());
      dataSource.setPassword(getPassword());
      return dataSource;
    }

    private String getUrl() {
      return String.format("jdbc:redshift://%s:%d/%s", getEndpoint(), getPort(), getDatabase());
    }

    @Nullable
    abstract String getEndpoint();
    @Nullable
    abstract Integer getPort();
    @Nullable
    abstract String getDatabase();
    @Nullable
    abstract String getUser();
    @Nullable
    abstract String getPassword();
    @Nullable
    abstract DataSource getDataSource();

    /**
     * Builds a {@link DataSourceConfiguration}.
     */
    @AutoValue.Builder
    public abstract static class Builder {

      abstract Builder setEndpoint(String value);
      abstract Builder setPort(Integer value);
      abstract Builder setDatabase(String value);
      abstract Builder setUser(String value);
      abstract Builder setPassword(String value);
      abstract Builder setDataSource(DataSource value);
      abstract DataSourceConfiguration build();
    }
  }

  /**
   * Interface that converts an object of type {@code T} to/from String arrays.
   */
  public interface RedshiftMarshaller<T> extends Serializable {

    T unmarshalFromRedshift(String[] value);
    String[] marshalToRedshift(T value);
  }

  /**
   * Write sends the input {@link PCollection} to a Redshift tablespec.
   *
   * <p>This {@link PTransform} writes the input {@link PCollection} to S3, then copies the S3
   * objects to a Redshift tablespec using the {@code COPY} command.
   *
   * <p>S3 cleanup is managed internally, but if this opration fails for any reason, the S3 objects
   * should be cleaned up manually.
   */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    abstract DataSourceConfiguration getDataSourceConfiguration();
    abstract Compression getCompression();
    abstract String getDestinationTableSpec();
    abstract String getS3TempLocationPrefix();
    abstract RedshiftMarshaller<T> getRedshiftMarshaller();
    abstract Coder<T> getCoder();

    /**
     * Creates a builder for {@link Write}.
     */
    public static <T> Builder<T> builder() {
      return new AutoValue_Redshift_Write.Builder<T>()
          .setCompression(Compression.GZIP);
    }

    /**
     * Builder for {@link Write}.
     */
    @AutoValue.Builder
    public abstract static class Builder<T> {

      /**
       * Sets the data source configuration.
       */
      public abstract Builder<T> setDataSourceConfiguration(DataSourceConfiguration value);

      /**
       * Sets the compression used when reading/writing intermediate files in S3; {@link
       * Compression#GZIP} by default.
       */
      public abstract Builder<T> setCompression(Compression value);

      /**
       * Sets the destination table name, and optional column list.
       */
      public abstract Builder<T> setDestinationTableSpec(String value);

      /**
       * Sets the S3 URI prefix for intermediate files.
       */
      public abstract Builder<T> setS3TempLocationPrefix(String value);

      /**
       * Sets the {@link RedshiftMarshaller}.
       */
      public abstract Builder<T> setRedshiftMarshaller(RedshiftMarshaller<T> value);

      /**
       * Sets the {@link Coder} for the resulting {@link PCollection}.
       */
      public abstract Builder<T> setCoder(Coder<T> value);

      /**
       * Builds a {@link Write} object.
       */
      public abstract Write<T> build();
    }

    @Override
    public PDone expand(PCollection<T> input) {

      PCollection<String> writtenS3Paths = input
          .apply("Marshal data to be copied",
              ParDo.of(new UnmarshalFromCsv<>(getRedshiftMarshaller())))
          .apply("PCollection to S3",
              TextIO.write()
                  .to(getS3TempLocationPrefix())
                  .withWritableByteChannelFactory(FileBasedSink.CompressionType.GZIP)
                  .<Void>withOutputFilenames())
          .getPerDestinationOutputFilenames()
          .apply(Values.create());

      PCollectionView<Void> copyComplete = writtenS3Paths
          .apply("Find common S3 prefix",
              Combine.globally(new BinaryCombineFn<String>() {
                @Override
                public String apply(String left, String right) {
                  return longestCommonPrefix(left, right);
                }
              }))
          .apply("Redshift COPY",
              ParDo.of(Copy.builder()
                  .setDataSourceConfiguration(getDataSourceConfiguration())
                  .setDestinationTableSpec(getDestinationTableSpec())
                  .setSourceCompression(getCompression())
                  .setDelimiter('|')
                  .build()))
          .apply(View.asSingleton());

      // Cleanup S3 files after they have been consumed.
      // Adding side input causes this subgraph to begin after COPY finishes.
      writtenS3Paths
          .apply(Combine.globally(new StringToListAccumulatorFn()))
          .apply("Clean up S3",
              ParDo.of(new Delete()).withSideInputs(copyComplete));

      return PDone.in(input.getPipeline());
    }
  }

  @VisibleForTesting
  static String longestCommonPrefix(String a, String b) {
    if (a == null || b == null) {
      return "";
    }
    int prefixLength = 0;
    while (a.length() > prefixLength && b.length() > prefixLength
        && a.charAt(prefixLength) == b.charAt(prefixLength)) {
      prefixLength++;
    }
    return a.substring(0, prefixLength);
  }

  /**
   * Read queries a Redshift database and returns the results as a {@link PCollection}.
   *
   * <p>This {@link PTransform} queries Redshift with the {@code UNLOAD} command, which generates
   * S3 objects, then reads the S3 objects as a {@link PCollection}.
   *
   * <p>S3 cleanup is managed internally, but if this operation failed for any reason, the S3
   * objects should be cleaned up manually.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract DataSourceConfiguration getDataSourceConfiguration();
    abstract Compression getCompression();
    abstract String getQuery();
    abstract String getS3TempLocationPrefix();
    abstract RedshiftMarshaller<T> getRedshiftMarshaller();
    abstract Coder<T> getCoder();

    public static <T> Builder<T> builder() {
      return new AutoValue_Redshift_Read.Builder<T>()
          .setCompression(Compression.GZIP);
    }

    /**
     * Builds a {@link Read} object.
     */
    @AutoValue.Builder
    public abstract static class Builder<T> {

      /**
       * Sets the data source configuration.
       */
      public abstract Builder<T> setDataSourceConfiguration(DataSourceConfiguration value);

      /**
       * Sets the compression used when reading/writing intermediate files in S3; {@link
       * Compression#GZIP} by default.
       */
      public abstract Builder<T> setCompression(Compression value);

      /**
       * Sets the source query used to generate the resulting {@link PCollection}.
       */
      public abstract Builder<T> setQuery(String value);

      /**
       * Sets the S3 URI prefix for intermediate files.
       */
      public abstract Builder<T> setS3TempLocationPrefix(String value);

      /**
       * Sets the {@link RedshiftMarshaller}.
       */
      public abstract Builder<T> setRedshiftMarshaller(RedshiftMarshaller<T> value);

      /**
       * Sets the {@link Coder} for the resulting {@link PCollection}.
       */
      public abstract Builder<T> setCoder(Coder<T> value);

      abstract Read<T> autoBuild();

      /**
       * Builds a {@link Copy} object.
       */
      public Read<T> build() {
        Read<T> read = autoBuild();
        checkArgument(!Strings.isNullOrEmpty(read.getQuery()), "query");
        return read;
      }
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      PCollection<String> unloadedS3Paths = input
          .apply("Null value seed",
              Create.of((Void) null)) // Guarantees one instance (or more, but not n instances)
          .apply("Redshift UNLOAD",
              ParDo.of(Unload.builder()
                  .setDataSourceConfiguration(getDataSourceConfiguration())
                  .setSourceQuery(getQuery())
                  .setDestination(getS3TempLocationPrefix())
                  .setDestinationCompression(getCompression())
                  .setDelimiter('|')
                  .build()
              ));

      PCollection<String> unloadedFileContents = unloadedS3Paths
          .apply("S3 objects to PCollection",
              TextIO.readAll()
                  .withCompression(getCompression())
                  .withMatchConfiguration(
                      MatchConfiguration.create(EmptyMatchTreatment.DISALLOW)));

      // Cleanup S3 files after they have been consumed.
      // Adding side input causes this subgraph to begin after TextIO.readAll() finishes.
      PCollectionView<Void> unloadComplete = unloadedFileContents
          .apply(Count.globally())
          .apply(MapElements.via(new SimpleFunction<Long, Void>() {
            @Override
            public Void apply(Long input) {
              return null;
            }
          }))
          .apply(View.asSingleton());

      unloadedS3Paths
          .apply(Combine.globally(new StringToListAccumulatorFn()))
          .apply("Clean up S3",
              ParDo.of(new Delete()).withSideInputs(unloadComplete));

      // Unmarshal and return.
      return unloadedFileContents
          .apply("Unmarshal unloaded data",
              ParDo.of(new MarshalToCsv<>(getRedshiftMarshaller())))
          .setCoder(getCoder());
    }
  }

  /**
   * Employs a {@link RedshiftMarshaller} object to convert CSV strings to {@code T} objects.
   */
  private static class UnmarshalFromCsv<T> extends DoFn<T, String> {

    private final ObjectWriter csvWriter;
    private final RedshiftMarshaller<T> marshaller;

    UnmarshalFromCsv(RedshiftMarshaller<T> marshaller) {
      CsvMapper mapper = new CsvMapper();
      CsvSchema schema = mapper.schema()
          .withColumnSeparator('|')
          .withNullValue(null)
          .withEscapeChar('\\')
          .withoutQuoteChar();
      csvWriter = mapper.writerFor(String[].class).with(schema);
      this.marshaller = marshaller;
    }

    @ProcessElement
    public void marshal(ProcessContext context) throws JsonProcessingException {
      String[] nullableValues = marshaller.marshalToRedshift(context.element());
      String[] values = FluentIterable.from(nullableValues)
          .transform(input -> {
            if (input == null) {
              return "NULL";
            }
            return input;
          }).toArray(String.class);
      String result = csvWriter.writeValueAsString(values);
      context.output(result);
    }
  }

  /**
   * Employs a {@link RedshiftMarshaller} object to convert {@code T} objects to CSV strings.
   */
  private static class MarshalToCsv<T> extends DoFn<String, T> {

    private final ObjectReader csvReader;
    private final RedshiftMarshaller<T> marshaller;

    MarshalToCsv(RedshiftMarshaller<T> marshaller) {
      CsvMapper mapper = new CsvMapper();
      CsvSchema schema = mapper.schema()
          .withColumnSeparator('|')
          .withNullValue(null)
          .withEscapeChar('\\')
          .withoutQuoteChar();
      csvReader = mapper.readerFor(String[].class).with(schema);
      this.marshaller = marshaller;
    }

    @ProcessElement
    public void unmarshal(ProcessContext context) throws IOException {
      String[] values = csvReader.readValue(context.element());
      String[] nullableValues = FluentIterable.from(values)
          .transform(input -> {
            if ("NULL".equals(input)) {
              return null;
            }
            return input;
          }).toArray(String.class);
      T result = marshaller.unmarshalFromRedshift(nullableValues);
      context.output(result);
    }
  }

  /**
   * Reduces {@code PCollection<String>} to {@code PCollection<List<String>>}.
   */
  private static class StringToListAccumulatorFn
      extends AccumulatingCombineFn<String, StringToListAccumulator, List<String>> {

    @Override
    public StringToListAccumulator createAccumulator() {
      return new StringToListAccumulator();
    }

    @Override
    public Coder<StringToListAccumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<String> inputCoder) {
      return DelegateCoder.of(
          ListCoder.of(StringUtf8Coder.of()),
          (CodingFunction<StringToListAccumulator, List<String>>)
              accumulator -> accumulator.accumulator,
          (CodingFunction<List<String>, StringToListAccumulator>)
              StringToListAccumulator::new);
    }
  }

  private static class StringToListAccumulator
      implements Accumulator<String, StringToListAccumulator, List<String>> {

    private final List<String> accumulator;

    private StringToListAccumulator() {
      this(new ArrayList<>());
    }

    private StringToListAccumulator(List<String> accumulator) {
      this.accumulator = accumulator;
    }

    @Override
    public void addInput(String value) {
      accumulator.add(value);
    }

    @Override
    public void mergeAccumulator(StringToListAccumulator other) {
      accumulator.addAll(other.accumulator);
    }

    @Override
    public List<String> extractOutput() {
      return accumulator;
    }
  }

  /**
   * Deletes files from S3.
   */
  private static class Delete extends DoFn<List<String>, Void> {

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      List<String> s3Paths = context.element();
      List<ResourceId> resourceIds = new ArrayList<>(s3Paths.size());

      List<MatchResult> matchResults =
          FileSystems.match(s3Paths, EmptyMatchTreatment.DISALLOW);
      for (MatchResult matchResult : matchResults) {
        for (Metadata metadata : matchResult.metadata()) {
          resourceIds.add(metadata.resourceId());
        }
      }

      FileSystems.delete(resourceIds);
    }
  }
}
