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
package org.apache.beam.sdk.io.csv;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.values.TypeDescriptors.rows;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Write.FileNaming;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;

/**
 * {@link PTransform}s for reading and writing CSV files.
 *
 * <h2>Reading CSV files</h2>
 *
 * <p>Reading from CSV files is not yet implemented. Please see <a
 * href="https://github.com/apache/beam/issues/24552">https://github.com/apache/beam/issues/24552</a>.
 *
 * <h2>Writing CSV files</h2>
 *
 * <p>To write a {@link PCollection} to one or more CSV files, use {@link CsvIO.Write}, using {@code
 * CsvIO.write().to(String)}. {@link CsvIO.Write} supports writing {@link Row} or custom Java types
 * using an inferred {@link Schema}. See the Beam Programming Guide on <a
 * href="https://beam.apache.org/documentation/programming-guide/#inferring-schemas">Inferring
 * schemas</a> for more information on how to enable Beam to infer a {@link Schema} from a custom
 * Java type.
 *
 * <p>Please note that {@link CsvIO.Write} restricts the use of flat {@link Schema}s and checks
 * whether the {@link Schema.FieldType}s are one of {@link CsvUtils#VALID_FIELD_TYPE_SET}.
 *
 * <h3>Example usage:</h3>
 *
 * <p>Suppose we have a <code>Transaction</code> class annotated with
 * {@code @DefaultSchema(JavaBeanSchema.class)} so that Beam can infer its {@link Schema}:
 *
 * <pre>{@code @DefaultSchema(JavaBeanSchema.class)
 * public class Transaction {
 *   public Transaction() { … }
 *   public Long getTransactionId();
 *   public void setTransactionId(Long transactionId) { … }
 *   public String getBank() { … }
 *   public void setBank(String bank) { … }
 *   public double getPurchaseAmount() { … }
 *   public void setPurchaseAmount(double purchaseAmount) { … }
 * }
 * }</pre>
 *
 * <p>From a {@code PCollection<Transaction>}, we can write one or many CSV files and {@link
 * CsvIO.Write} will automatically create the header based on its inferred {@link Schema}
 *
 * <pre>{@code
 * PCollection<Transaction> transactions ...
 * transactions.apply(CsvIO.<Transaction>write().to("gs://bucket/path/to/folder/prefix"));
 * }</pre>
 *
 * <p>The resulting CSV files will look like the following where the header is repeated for every
 * file, whereas by default, {@link CsvIO.Write} will write all fields in <b>sorted order</b> of the
 * field names.
 *
 * <pre>{@code
 * bank,purchaseAmount,transactionId
 * A,10.23,12345
 * B,54.65,54321
 * C,11.76,98765
 * }</pre>
 *
 * <p>{@link CsvIO.Write} allows the use a subset of the available fields or control the order of
 * their output using {@link CsvIO.Write#withSchemaFields(List)} listing of matching {@link
 * Schema#getField(String)} names.
 *
 * <pre>{@code
 * PCollection<Transaction> transactions ...
 * transactions.apply(
 *  CsvIO
 *    .<Transaction>write()
 *    .to("gs://bucket/path/to/folder/prefix")
 *    .withSchemaFields(Arrays.asList("transactionId", "purchaseAmount"))
 * );
 * }</pre>
 *
 * <p>The resulting CSV files will look like the following where the header is repeated for every
 * file, but only including the subset of fields in their listed order.
 *
 * <pre>{@code
 * transactionId,purchaseAmount
 * 12345,10.23
 * 54321,54.65
 * 98765,11.76
 * }</pre>
 *
 * <p>Additionally, we may define a {@link CSVFormat} other than the {@link CSVFormat#DEFAULT}.
 *
 * <pre>{@code
 * PCollection<Transaction> transactions ...
 * transactions.apply(
 *  CsvIO
 *    .<Transaction>write()
 *    .to("gs://bucket/path/to/folder/prefix")
 *    .withCSVFormat(CSVFormat.POSTGRESQL_CSV)
 *  );
 * }</pre>
 *
 * <p>The resulting CSV files will look like the following formatted according to {@link
 * CSVFormat#POSTGRESQL_CSV}.
 *
 * <pre>{@code
 * "bank","purchaseAmount","transactionId"
 * "A","10.23","12345"
 * "B","54.65","54321"
 * "C","11.76","98765"
 * }</pre>
 *
 * <p>A {@link PCollection} of {@link Row}s works just like custom Java types illustrated above,
 * except we use {@link CsvIO#writeRows()} as shown below for the same {@code Transaction} class. We
 * derive {@code Transaction}'s {@link Schema} using {@link
 * org.apache.beam.sdk.schemas.annotations.DefaultSchema.DefaultSchemaProvider}.
 *
 * <pre>{@code
 * DefaultSchemaProvider defaultSchemaProvider = new DefaultSchemaProvider();
 * Schema schema = defaultSchemaProvider.schemaFor(TypeDescriptor.of(Transaction.class));
 * PCollection<Row> transactions = pipeline.apply(Create.of(
 *  Row
 *    .withSchema(schema)
 *    .withFieldValue("bank", "A")
 *    .withFieldValue("purchaseAmount", 10.23)
 *    .withFieldValue("transactionId", "12345")
 *    .build(),
 *  Row
 *    .withSchema(schema)
 *    .withFieldValue("bank", "B")
 *    .withFieldValue("purchaseAmount", 54.65)
 *    .withFieldValue("transactionId", "54321")
 *    .build(),
 *  Row
 *    .withSchema(schema)
 *    .withFieldValue("bank", "C")
 *    .withFieldValue("purchaseAmount", 11.76)
 *    .withFieldValue("transactionId", "98765")
 *    .build()
 * );
 *
 * transactions.apply(CsvIO.writeRows().to("gs://bucket/path/to/folder/prefix"));
 * }</pre>
 *
 * <p>Writing the transactions {@link PCollection} of {@link Row}s would yield the following CSV
 * file content.
 *
 * <pre>{@code
 * bank,purchaseAmount,transactionId
 * A,10.23,12345
 * B,54.65,54321
 * C,11.76,98765
 * }</pre>
 */
public class CsvIO {

  static final String DEFAULT_FILENAME_SUFFIX = ".csv";

  /** Instantiates a {@link Write} for writing user types in CSV format. */
  public static <T> Write<T> write() {
    return new AutoValue_CsvIO_Write.Builder<T>().setFileWrite(FileIO.write()).build();
  }

  /** Instantiates a {@link Write} for {@link Row}s in CSV format. */
  public static Write<Row> writeRows() {
    return new AutoValue_CsvIO_Write.Builder<Row>().setFileWrite(FileIO.write()).build();
  }

  /** Implementation of {@link FileIO.Sink}. */
  @AutoValue
  public abstract static class Sink<T> implements FileIO.Sink<T> {

    public static <T> Builder<T> builder() {
      return new AutoValue_CsvIO_Sink.Builder<>();
    }

    /**
     * Not to be confused with the CSV header, it is content written to the top of every sharded
     * file prior to the header. In the example below, all the text proceeding the header
     * 'column1,column2,column3' is the preamble.
     *
     * <pre>{@code
     * Fake company, Inc.
     * Lab experiment: abcdefg123456
     * Experiment date: 2022-12-05
     * Operator: John Doe
     * column1,column2,colum3
     * 1,2,3
     * 4,5,6
     * }</pre>
     */
    public Sink<T> withPreamble(String preamble) {
      return toBuilder().setPreamble(preamble).build();
    }

    private transient Optional<PrintWriter> writer = Optional.empty();

    /**
     * Opens a {@link WritableByteChannel} for writing CSV files. Writes the {@link #getPreamble()}
     * if available followed by the {@link #getHeader()}.
     */
    @Override
    public void open(WritableByteChannel channel) throws IOException {
      writer =
          Optional.of(
              new PrintWriter(
                  new BufferedWriter(
                      new OutputStreamWriter(Channels.newOutputStream(channel), UTF_8))));
      if (getPreamble() != null) {
        writer.get().println(getPreamble());
      }
      writer.get().println(getHeader());
    }

    /** Serializes and writes the element to a file. */
    @Override
    public void write(T element) throws IOException {
      checkState(writer.isPresent());
      String line = getFormatFunction().apply(element);
      writer.get().println(line);
    }

    @Override
    public void flush() throws IOException {
      checkState(writer.isPresent());
      writer.get().flush();
    }

    /**
     * Not to be confused with the header. It is the text preceding the header in every sharded
     * file. See {@link #withPreamble(String)}.
     */
    abstract String getPreamble();

    /**
     * The column names of the CSV file written at the top line of each shard after the preamble, if
     * available. Named fields in header must conform to the types listed in {@link
     * CsvUtils#VALID_FIELD_TYPE_SET}.
     */
    abstract String getHeader();

    /** A {@link SerializableFunction} for converting to a CSV formatted string. */
    abstract SerializableFunction<T, String> getFormatFunction();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      /**
       * Not to be confused with the header. It is the text preceding the header in every sharded
       * file. See {@link #withPreamble(String)}.
       */
      abstract Builder<T> setPreamble(String value);

      abstract Optional<String> getPreamble();

      /**
       * The column names of the CSV file written at the top line of each shard after the preamble,
       * if available. Named fields in header must conform to the types listed in {@link
       * CsvUtils#VALID_FIELD_TYPE_SET}.
       */
      abstract Builder<T> setHeader(String value);

      /** A {@link SerializableFunction} for converting a to a CSV formatted string. */
      abstract Builder<T> setFormatFunction(SerializableFunction<T, String> value);

      abstract Sink<T> autoBuild();

      final Sink<T> build() {
        if (!getPreamble().isPresent()) {
          setPreamble("");
        }
        return autoBuild();
      }
    }
  }

  /** {@link PTransform} for writing CSV files. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    /** Specifies a common prefix for all generated files. */
    public Write<T> to(String filenamePrefix) {
      Path path = Paths.get(filenamePrefix);
      String directory = "";
      String name = "";
      if (path.getParent() != null) {
        directory = path.getParent().toString();
      }
      if (path.getFileName() != null) {
        name = path.getFileName().toString();
      }
      return toBuilder()
          .setFileWrite(
              FileIO.<Row>write()
                  .to(directory)
                  .withPrefix(name)
                  .withSuffix(DEFAULT_FILENAME_SUFFIX))
          .build();
    }

    /** The {@link CSVFormat} of the destination CSV file data. */
    public Write<T> withCSVFormat(CSVFormat format) {
      return toBuilder().setCSVFormat(format).build();
    }

    /** Controls the subset and order of included fields when writing records. */
    public Write<T> withSchemaFields(List<String> schemaFields) {
      return toBuilder().setSchemaFields(schemaFields).build();
    }

    /**
     * Not to be confused with the CSV header, it is content written to the top of every sharded
     * file prior to the header. In the example below, all the text proceeding the header
     * 'column1,column2,column3' is the preamble.
     *
     * <pre>{@code
     * Fake company, Inc.
     * Lab experiment: abcdefg123456
     * Experiment date: 2022-12-05
     * Operator: John Doe
     * column1,column2,colum3
     * 1,2,3
     * 4,5,6
     * }</pre>
     */
    public Write<T> withPreamble(String preamble) {
      return toBuilder().setPreamble(preamble).build();
    }

    /** Specifies the {@link Compression} of all generated shard files. */
    public Write<T> withCompression(Compression compression) {
      return toBuilder().setFileWrite(getFileWrite().withCompression(compression)).build();
    }

    /**
     * Specifies to use a given fixed number of shards per window. See {@link
     * FileIO.Write#withNumShards(int)} for details.
     */
    public Write<T> withNumShards(Integer numShards) {
      return toBuilder().setFileWrite(getFileWrite().withNumShards(numShards)).build();
    }

    /**
     * The sharding {@link PTransform} to use with the underlying {@link FileIO.Write}. Since {@link
     * CsvIO.Write} converts user types to {@link Row}, the sharding transform is limited to {@link
     * PCollection} of {@link Row}s.
     */
    public CsvIO.Write<T> withSharding(
        PTransform<PCollection<Row>, PCollectionView<Integer>> sharding) {
      return toBuilder().setFileWrite(getFileWrite().withSharding(sharding)).build();
    }

    /**
     * The {@link Contextful} destinationFn to use with the underlying {@link FileIO.Write}. See
     * {@link FileIO.Write#by(Contextful)} for more details.
     */
    public CsvIO.Write<T> by(Contextful<Contextful.Fn<Row, Void>> destinationFn) {
      return toBuilder().setFileWrite(getFileWrite().by(destinationFn)).build();
    }

    /**
     * The {@link Contextful} namingFn to use with the underlying {@link FileIO.Write}. See {@link
     * FileIO.Write#withNaming(Contextful)} for more details.
     */
    public CsvIO.Write<T> withNaming(
        Contextful<Contextful.Fn<Void, FileIO.Write.FileNaming>> namingFn) {
      return toBuilder().setFileWrite(getFileWrite().withNaming(namingFn)).build();
    }

    /**
     * The {@link FileIO.Write.FileNaming} naming to use with the underlying {@link FileIO.Write}.
     * See {@link FileIO.Write#withNaming(FileNaming)} for more details.
     */
    public CsvIO.Write<T> withNaming(FileNaming naming) {
      return toBuilder().setFileWrite(getFileWrite().withNaming(naming)).build();
    }

    /**
     * The String prefix to use with the underlying {@link FileIO.Write}. See {@link
     * FileIO.Write#withPrefix(String)} for more details.
     */
    public CsvIO.Write<T> withPrefix(String prefix) {
      return toBuilder().setFileWrite(getFileWrite().withPrefix(prefix)).build();
    }

    /**
     * Determines whether to use {@link FileIO.Write#withNoSpilling()} with the underlying {@link
     * FileIO.Write}.
     */
    public CsvIO.Write<T> withNoSpilling() {
      return toBuilder().setFileWrite(getFileWrite().withNoSpilling()).build();
    }

    abstract FileIO.Write<Void, Row> getFileWrite();

    abstract CSVFormat getCSVFormat();

    abstract String getPreamble();

    abstract List<String> getSchemaFields();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setFileWrite(FileIO.Write<Void, Row> value);

      abstract Optional<FileIO.Write<Void, Row>> getFileWrite();

      abstract Builder<T> setCSVFormat(CSVFormat value);

      abstract Optional<CSVFormat> getCSVFormat();

      abstract Builder<T> setPreamble(String value);

      abstract Builder<T> setSchemaFields(List<String> value);

      abstract Optional<List<String>> getSchemaFields();

      abstract Write<T> autoBuild();

      final Write<T> build() {
        if (!getCSVFormat().isPresent()) {
          setCSVFormat(CSVFormat.DEFAULT);
        }

        if (!getSchemaFields().isPresent()) {
          setSchemaFields(Collections.emptyList());
        }

        return autoBuild();
      }
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (!input.hasSchema()) {
        throw new IllegalArgumentException(
            String.format(
                "%s requires an input Schema. Note that only Row or user classes are supported. Consider using TextIO or FileIO directly when writing primitive types",
                Write.class.getName()));
      }

      Schema schema = input.getSchema();
      SerializableFunction<T, Row> toRowFn = input.getToRowFunction();
      PCollection<Row> rows =
          input.apply(MapElements.into(rows()).via(toRowFn)).setRowSchema(schema);
      rows.apply("writeRowsToCsv", getFileWrite().via(buildSink(schema)));
      return PDone.in(input.getPipeline());
    }

    /** Builds a {@link Sink} for writing {@link Row} serialized using {@link CSVFormat}. */
    Sink<Row> buildSink(Schema schema) {
      String header = CsvUtils.buildHeaderFrom(getOrDeriveSchemaFields(schema), getCSVFormat());
      return Sink.<Row>builder()
          .setPreamble(getPreamble())
          .setHeader(header)
          .setFormatFunction(
              CsvUtils.getRowToCsvStringFunction(schema, getCSVFormat(), getSchemaFields()))
          .build();
    }

    private List<String> getOrDeriveSchemaFields(Schema schema) {
      if (getSchemaFields().isEmpty()) {
        return schema.sorted().getFieldNames();
      }
      return getSchemaFields();
    }
  }
}
