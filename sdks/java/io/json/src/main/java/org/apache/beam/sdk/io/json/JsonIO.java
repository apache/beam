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
package org.apache.beam.sdk.io.json;

import static org.apache.beam.sdk.values.TypeDescriptors.rows;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import com.google.auto.value.AutoValue;
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
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * {@link PTransform}s for reading and writing JSON files.
 *
 * <h2>Reading JSON files</h2>
 *
 * <p>Reading from JSON files is not yet implemented in Java. Please see <a
 * href="https://github.com/apache/beam/issues/24552">https://github.com/apache/beam/issues/24552</a>.
 *
 * <h2>Writing JSON files</h2>
 *
 * <p>To write a {@link PCollection} to one or more line-delimited JSON files, use {@link
 * JsonIO.Write}, using{@link JsonIO#writeRows} or {@link JsonIO#write}. {@link JsonIO.Write}
 * supports writing {@link Row} or custom Java types using an inferred {@link Schema}. Examples
 * below show both scenarios. See the Beam Programming Guide on <a
 * href="https://beam.apache.org/documentation/programming-guide/#inferring-schemas">inferring
 * schemas</a> for more information on how to enable Beam to infer a {@link Schema} from a custom
 * Java type.
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
 * <p>From a {@code PCollection<Transaction>}, {@link JsonIO.Write} can write one or many JSON
 * files.
 *
 * <pre>{@code
 * PCollection<Transaction> transactions = ...
 * transactions.apply(JsonIO.<Transaction>write("path/to/folder/prefix"));
 * }</pre>
 *
 * <p>The resulting JSON files will look like the following where the header is repeated for every
 * file, whereas by default, {@link JsonIO.Write} will write all fields in <b>sorted order</b> of
 * the field names.
 *
 * <pre>{@code
 * {"bank": "A", "purchaseAmount": 10.23, "transactionId": 12345}
 * {"bank": "B", "purchaseAmount": 54.65, "transactionId": 54321}
 * {"bank": "C", "purchaseAmount": 11,76, "transactionId": 98765}
 * }</pre>
 *
 * <p>A {@link PCollection} of {@link Row}s works just like custom Java types illustrated above,
 * except we use {@link JsonIO#writeRows} as shown below for the same {@code Transaction} class. We
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
 *  JsonIO
 *    .writeRowsTo("gs://bucket/path/to/folder/prefix")
 * );
 * }</pre>
 *
 * <p>Writing the transactions {@link PCollection} of {@link Row}s would yield the following JSON
 * file content.
 *
 * <pre>{@code
 * {"bank": "A", "purchaseAmount": 10.23, "transactionId": 12345}
 * {"bank": "B", "purchaseAmount": 54.65, "transactionId": 54321}
 * {"bank": "C", "purchaseAmount": 11,76, "transactionId": 98765}
 * }</pre>
 */
public class JsonIO {
  static final String DEFAULT_FILENAME_SUFFIX = ".json";

  /** Instantiates a {@link Write} for writing user types in {@link JSONFormat} format. */
  public static <T> Write<T> write(String to) {
    return new AutoValue_JsonIO_Write.Builder<T>()
        .setTextIOWrite(createDefaultTextIOWrite(to))
        .build();
  }

  /** Instantiates a {@link Write} for writing {@link Row}s in {@link JSONFormat} format. */
  public static Write<Row> writeRows(String to) {
    return new AutoValue_JsonIO_Write.Builder<Row>()
        .setTextIOWrite(createDefaultTextIOWrite(to))
        .build();
  }

  /** {@link PTransform} for writing JSON files. */
  @AutoValue
  public abstract static class Write<T>
      extends PTransform<PCollection<T>, WriteFilesResult<String>> {

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

    /** The underlying {@link FileIO.Write} that writes converted input to JSON formatted output. */
    abstract TextIO.Write getTextIOWrite();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      /**
       * The underlying {@link FileIO.Write} that writes converted input to JSON formatted output.
       */
      abstract Builder<T> setTextIOWrite(TextIO.Write value);

      abstract Write<T> autoBuild();

      final Write<T> build() {
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

      SerializableFunction<Row, String> toJsonFn =
          JsonUtils.getRowToJsonStringsFunction(input.getSchema());

      PCollection<String> json = rows.apply("To JSON", MapElements.into(strings()).via(toJsonFn));

      return json.apply("Write JSON", getTextIOWrite().withOutputFilenames());
    }
  }

  private static TextIO.Write createDefaultTextIOWrite(String to) {
    return TextIO.write().to(to).withSuffix(DEFAULT_FILENAME_SUFFIX);
  }
}
