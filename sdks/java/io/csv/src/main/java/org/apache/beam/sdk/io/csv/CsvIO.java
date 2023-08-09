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

import static java.util.Objects.requireNonNull;
import static org.apache.beam.sdk.values.TypeDescriptors.rows;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.ShardNameTemplate;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
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
 * <p>To write a {@link PCollection} to one or more CSV files, use {@link CsvIO.Write}, using {@link
 * CsvIO#writeRows} or {@link CsvIO#write}. {@link CsvIO.Write} supports writing {@link Row} or
 * custom Java types using an inferred {@link Schema}. Examples below show both scenarios. See the
 * Beam Programming Guide on <a
 * href="https://beam.apache.org/documentation/programming-guide/#inferring-schemas">inferring
 * schemas</a> for more information on how to enable Beam to infer a {@link Schema} from a custom
 * Java type.
 *
 * <p>{@link CsvIO.Write} only supports writing the parts of {@link Schema} aware types that do not
 * contain any nested {@link FieldType}s such a {@link
 * org.apache.beam.sdk.schemas.Schema.TypeName#ROW} or repeated {@link
 * org.apache.beam.sdk.schemas.Schema.TypeName#ARRAY} types. See {@link
 * CsvIO.Write#VALID_FIELD_TYPE_SET} for valid {@link FieldType}s.
 *
 * <h3>Example usage:</h3>
 *
 * <p>Suppose we have the following <code>Transaction</code> class annotated with
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
 * <p>From a {@code PCollection<Transaction>}, {@link CsvIO.Write} can write one or many CSV files
 * automatically creating the header based on its inferred {@link Schema}.
 *
 * <pre>{@code
 * PCollection<Transaction> transactions = ...
 * transactions.apply(CsvIO.<Transaction>write("path/to/folder/prefix", CSVFormat.DEFAULT));
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
 * <p>To control the order and subset of fields that {@link CsvIO.Write} writes, use {@link
 * CSVFormat#withHeader}. Note, however, the following constraints:
 *
 * <ol>
 *   <li>Each header column must match a field name in the {@link Schema}; matching is case
 *       sensitive.
 *   <li>Matching header columns must match {@link Schema} fields that are valid {@link FieldType}s;
 *       see {@link #VALID_FIELD_TYPE_SET}.
 *   <li>{@link CSVFormat} only allows repeated header columns when {@link
 *       CSVFormat#withAllowDuplicateHeaderNames}
 * </ol>
 *
 * <p>The following example shows the use of {@link CSVFormat#withHeader} to control the order and
 * subset of <code>Transaction</code> fields.
 *
 * <pre>{@code
 * PCollection<Transaction> transactions ...
 * transactions.apply(
 *  CsvIO
 *    .<Transaction>write("path/to/folder/prefix", CSVFormat.DEFAULT.withHeader("transactionId", "purchaseAmount"))
 * );
 * }</pre>
 *
 * <p>The resulting CSV files will look like the following where the header is repeated for every
 * file, but will only include the subset of fields in their listed order.
 *
 * <pre>{@code
 * transactionId,purchaseAmount
 * 12345,10.23
 * 54321,54.65
 * 98765,11.76
 * }</pre>
 *
 * <p>In addition to header customization, {@link CsvIO.Write} supports {@link
 * CSVFormat#withHeaderComments} as shown below. Note that {@link CSVFormat#withCommentMarker} is
 * required when specifying header comments.
 *
 * <pre>{@code
 * PCollection<Transaction> transactions = ...
 * transactions
 *    .apply(
 *        CsvIO.<Transaction>write("path/to/folder/prefix",
 *        CSVFormat.DEFAULT
 *          .withCommentMarker('#')
 *          .withHeaderComments("Bank Report", "1970-01-01", "Operator: John Doe")
 *    );
 * }</pre>
 *
 * <p>The resulting CSV files will look like the following where the header and header comments are
 * repeated for every shard file.
 *
 * <pre>{@code
 * # Bank Report
 * # 1970-01-01
 * # Operator: John Doe
 * bank,purchaseAmount,transactionId
 * A,10.23,12345
 * B,54.65,54321
 * C,11.76,98765
 * }</pre>
 *
 * <p>A {@link PCollection} of {@link Row}s works just like custom Java types illustrated above,
 * except we use {@link CsvIO#writeRows} as shown below for the same {@code Transaction} class. We
 * derive {@code Transaction}'s {@link Schema} using a {@link
 * org.apache.beam.sdk.schemas.annotations.DefaultSchema.DefaultSchemaProvider}. Note that
 * hard-coding the {@link Row}s below is for illustration purposes. Developers are instead
 * encouraged to take advantage of {@link
 * org.apache.beam.sdk.schemas.annotations.DefaultSchema.DefaultSchemaProvider#toRowFunction}.
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
 * transactions.apply(
 *  CsvIO
 *    .writeRowsTo("gs://bucket/path/to/folder/prefix", CSVFormat.DEFAULT)
 * );
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
 *
 * {@link CsvIO.Write} does not support the following {@link CSVFormat} properties and will throw an
 * {@link IllegalArgumentException}.
 *
 * <ul>
 *   <li>{@link CSVFormat#withAllowMissingColumnNames}
 *   <li>{@link CSVFormat#withAutoFlush}
 *   <li>{@link CSVFormat#withIgnoreHeaderCase}
 *   <li>{@link CSVFormat#withIgnoreSurroundingSpaces}
 * </ul>
 */
public class CsvIO {
  /**
   * The valid {@link Schema.FieldType} from which {@link CsvIO} converts CSV records to the fields.
   *
   * <ul>
   *   <li>{@link FieldType#BYTE}
   *   <li>{@link FieldType#BOOLEAN}
   *   <li>{@link FieldType#DATETIME}
   *   <li>{@link FieldType#DECIMAL}
   *   <li>{@link FieldType#DOUBLE}
   *   <li>{@link FieldType#INT16}
   *   <li>{@link FieldType#INT32}
   *   <li>{@link FieldType#INT64}
   *   <li>{@link FieldType#FLOAT}
   *   <li>{@link FieldType#STRING}
   * </ul>
   */
  public static final Set<Schema.FieldType> VALID_FIELD_TYPE_SET =
      ImmutableSet.of(
          FieldType.BYTE,
          FieldType.BOOLEAN,
          FieldType.DATETIME,
          FieldType.DECIMAL,
          FieldType.DOUBLE,
          FieldType.INT16,
          FieldType.INT32,
          FieldType.INT64,
          FieldType.FLOAT,
          FieldType.STRING);

  static final String DEFAULT_FILENAME_SUFFIX = ".csv";

  /** Instantiates a {@link Write} for writing user types in {@link CSVFormat} format. */
  public static <T> Write<T> write(String to, CSVFormat csvFormat) {
    return new AutoValue_CsvIO_Write.Builder<T>()
        .setTextIOWrite(createDefaultTextIOWrite(to))
        .setCSVFormat(csvFormat)
        .build();
  }

  /** Instantiates a {@link Write} for writing {@link Row}s in {@link CSVFormat} format. */
  public static Write<Row> writeRows(String to, CSVFormat csvFormat) {
    return new AutoValue_CsvIO_Write.Builder<Row>()
        .setTextIOWrite(createDefaultTextIOWrite(to))
        .setCSVFormat(csvFormat)
        .build();
  }

  /** {@link PTransform} for writing CSV files. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, WriteFilesResult<String>>
      implements HasDisplayData {

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      CSVFormat csvFormat = getCSVFormat();
      builder.add(DisplayData.item("delimiter", String.valueOf(csvFormat.getDelimiter())));
      if (csvFormat.getQuoteCharacter() != null) {
        builder.add(
            DisplayData.item("quoteCharacter", String.valueOf(csvFormat.getQuoteCharacter())));
      }
      if (csvFormat.getQuoteMode() != null) {
        builder.add(DisplayData.item("quoteMode", csvFormat.getQuoteMode().toString()));
      }
      if (csvFormat.getCommentMarker() != null) {
        builder.add(DisplayData.item("commentMarker", csvFormat.getCommentMarker().toString()));
      }
      if (csvFormat.getEscapeCharacter() != null) {
        builder.add(DisplayData.item("escapeCharacter", csvFormat.getEscapeCharacter().toString()));
      }
      builder.addIfNotNull(DisplayData.item("recordSeparator", csvFormat.getRecordSeparator()));
      builder.addIfNotNull(DisplayData.item("nullString", csvFormat.getNullString()));
      if (csvFormat.getHeaderComments() != null) {
        builder.add(
            DisplayData.item("headerComments", String.join("\n", csvFormat.getHeaderComments())));
      }
      if (csvFormat.getHeader() != null) {
        builder.add(DisplayData.item("header", String.join(",", csvFormat.getHeader())));
      }
      builder.addIfNotNull(DisplayData.item("trailingDelimiter", csvFormat.getTrailingDelimiter()));
      builder.addIfNotNull(DisplayData.item("trim", csvFormat.getTrim()));
      builder.addIfNotNull(
          DisplayData.item("allowDuplicateHeaderNames", csvFormat.getAllowDuplicateHeaderNames()));
    }

    /** Specifies the {@link Compression} of all generated shard files. */
    public Write<T> withCompression(Compression compression) {
      return toBuilder().setTextIOWrite(getTextIOWrite().withCompression(compression)).build();
    }

    /** Whether to skip the spilling of data. See {@link WriteFiles#withNoSpilling}. */
    public Write<T> withNoSpilling() {
      return toBuilder().setTextIOWrite(getTextIOWrite().withNoSpilling()).build();
    }

    /**
     * Specifies to use a given fixed number of shards per window. See {@link
     * TextIO.Write#withNumShards}.
     */
    public Write<T> withNumShards(Integer numShards) {
      return toBuilder().setTextIOWrite(getTextIOWrite().withNumShards(numShards)).build();
    }

    /**
     * Forces a single file as output and empty shard name template. See {@link
     * TextIO.Write#withoutSharding}.
     */
    public Write<T> withoutSharding() {
      return toBuilder().setTextIOWrite(getTextIOWrite().withoutSharding()).build();
    }

    /**
     * Uses the given {@link ShardNameTemplate} for naming output files. See {@link
     * TextIO.Write#withShardNameTemplate}.
     */
    public Write<T> withShardTemplate(String shardTemplate) {
      return toBuilder()
          .setTextIOWrite(getTextIOWrite().withShardNameTemplate(shardTemplate))
          .build();
    }

    /** Configures the filename suffix for written files. See {@link TextIO.Write#withSuffix}. */
    public Write<T> withSuffix(String suffix) {
      return toBuilder().setTextIOWrite(getTextIOWrite().withSuffix(suffix)).build();
    }

    /**
     * Set the base directory used to generate temporary files. See {@link
     * TextIO.Write#withTempDirectory}.
     */
    public Write<T> withTempDirectory(ResourceId tempDirectory) {
      return toBuilder().setTextIOWrite(getTextIOWrite().withTempDirectory(tempDirectory)).build();
    }

    /**
     * Preserves windowing of input elements and writes them to files based on the element's window.
     * See {@link TextIO.Write#withWindowedWrites}.
     */
    public Write<T> withWindowedWrites() {
      return toBuilder().setTextIOWrite(getTextIOWrite().withWindowedWrites()).build();
    }

    /**
     * Returns a transform for writing to text files like this one but that has the given {@link
     * FileBasedSink.WritableByteChannelFactory} to be used by the {@link FileBasedSink} during
     * output. See {@link TextIO.Write#withWritableByteChannelFactory}.
     */
    public Write<T> withWritableByteChannelFactory(
        FileBasedSink.WritableByteChannelFactory writableByteChannelFactory) {
      return toBuilder()
          .setTextIOWrite(
              getTextIOWrite().withWritableByteChannelFactory(writableByteChannelFactory))
          .build();
    }

    /** The underlying {@link FileIO.Write} that writes converted input to CSV formatted output. */
    abstract TextIO.Write getTextIOWrite();

    /** The {@link CSVFormat} to inform headers, header comments, and format CSV row content. */
    abstract CSVFormat getCSVFormat();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      /**
       * The underlying {@link FileIO.Write} that writes converted input to CSV formatted output.
       */
      abstract Builder<T> setTextIOWrite(TextIO.Write value);

      /** The {@link CSVFormat} to convert input. Defaults to {@link CSVFormat#DEFAULT}. */
      abstract Builder<T> setCSVFormat(CSVFormat value);

      abstract CSVFormat getCSVFormat();

      abstract Write<T> autoBuild();

      final Write<T> build() {

        if (getCSVFormat().getHeaderComments() != null) {
          checkArgument(
              getCSVFormat().isCommentMarkerSet(),
              "CSVFormat withCommentMarker required when withHeaderComments");
        }

        return autoBuild();
      }
    }

    @Override
    public WriteFilesResult<String> expand(PCollection<T> input) {
      if (!input.hasSchema()) {
        throw new IllegalArgumentException(
            String.format(
                "%s requires an input Schema. Note that only Row or user classes are supported. Consider using TextIO or FileIO directly when writing primitive types",
                Write.class.getName()));
      }

      Schema schema = input.getSchema();

      RowCoder rowCoder = RowCoder.of(schema);

      PCollection<Row> rows =
          input
              .apply("To Rows", MapElements.into(rows()).via(input.getToRowFunction()))
              .setCoder(rowCoder);

      CSVFormat csvFormat = buildHeaderFromSchemaIfNeeded(getCSVFormat(), schema);

      TextIO.Write write = getTextIOWrite();

      write = writeWithCSVFormatHeaderAndComments(csvFormat, write);

      SerializableFunction<Row, String> toCsvFn =
          CsvRowConversions.RowToCsv.builder()
              .setCSVFormat(csvFormat)
              .setSchema(input.getSchema())
              .build();

      PCollection<String> csv = rows.apply("To CSV", MapElements.into(strings()).via(toCsvFn));

      return csv.apply("Write CSV", write.withOutputFilenames());
    }

    private static CSVFormat buildHeaderFromSchemaIfNeeded(CSVFormat csvFormat, Schema schema) {
      if (csvFormat.getHeader() == null) {
        csvFormat = csvFormat.withHeader(schema.sorted().getFieldNames().toArray(new String[0]));
      }

      return csvFormat;
    }

    private static TextIO.Write writeWithCSVFormatHeaderAndComments(
        CSVFormat csvFormat, TextIO.Write write) {

      if (csvFormat.getSkipHeaderRecord()) {
        return write;
      }

      String[] header = requireNonNull(csvFormat.getHeader());
      List<String> result = new ArrayList<>();
      if (csvFormat.getHeaderComments() != null) {
        for (String comment : csvFormat.getHeaderComments()) {
          result.add(csvFormat.getCommentMarker() + " " + comment);
        }
      }

      // We remove header comments since we've already processed them above.
      // CSV commons library was designed with a single file write in mind
      // which is why we need to manipulate CSVFormat in this way to be compatible
      // in a TextIO.Write context.
      CSVFormat withHeaderCommentsRemoved = csvFormat.withHeaderComments();

      result.add(
          withHeaderCommentsRemoved
              // The withSkipHeaderRecord parameter prevents CSVFormat from outputting two copies of
              // the header.
              .withSkipHeaderRecord()
              .format((Object[]) header));

      return write.withHeader(String.join("\n", result));
    }
  }

  private static TextIO.Write createDefaultTextIOWrite(String to) {
    return TextIO.write().to(to).withSuffix(DEFAULT_FILENAME_SUFFIX);
  }
}
