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
package org.apache.beam.sdk.io.kudu;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.kudu.Common;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.RowResult;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bounded source and sink for Kudu.
 *
 * <p>For more information, see the online documentation at <a
 * href="https://kudu.apache.org/">Kudu</a>.
 *
 * <h3>Reading from Kudu</h3>
 *
 * <p>{@code KuduIO} provides a source to read and returns a bounded collection of entities as
 * {@code PCollection&lt;T&gt;}. An entity is built by parsing a Kudu {@link RowResult} using the
 * provided {@link SerializableFunction}.
 *
 * <p>The following example illustrates various options for configuring the IO:
 *
 * <pre>{@code
 * pipeline.apply(
 *     KuduIO.<String>read()
 *         .withMasterAddresses("kudu1:8051,kudu2:8051,kudu3:8051")
 *         .withTable("table")
 *         .withParseFn(
 *             (SerializableFunction<RowResult, String>) input -> input.getString(COL_NAME))
 *         .withCoder(StringUtf8Coder.of()));
 *     // above options illustrate a typical minimum set, returns PCollection<String>
 * }</pre>
 *
 * <p>{@code withCoder(...)} may be omitted if it can be inferred from the @{CoderRegistry}.
 * However, when using a Lambda Expression or an anonymous inner class to define the function, type
 * erasure will prohibit this. In such cases you are required to explicitly set the coder as in the
 * above example.
 *
 * <p>Optionally, you can provide {@code withPredicates(...)} to apply a query to filter rows from
 * the kudu table.
 *
 * <p>Optionally, you can provide {@code withProjectedColumns(...)} to limit the columns returned
 * from the Kudu scan to improve performance. The columns required in the {@code ParseFn} must be
 * declared in the projected columns.
 *
 * <p>Optionally, you can provide {@code withBatchSize(...)} to set the number of bytes returned
 * from the Kudu scanner in each batch.
 *
 * <p>Optionally, you can provide {@code withFaultTolerent(...)} to enforce the read scan to resume
 * a scan on another tablet server if the current server fails.
 *
 * <h3>Writing to Kudu</h3>
 *
 * <p>The Kudu sink executes a set of operations on a single table. It takes as input a {@link
 * PCollection PCollection} and a {@link FormatFunction} which is responsible for converting the
 * input into an idempotent transformation on a row.
 *
 * <p>To configure a Kudu sink, you must supply the Kudu master addresses, the table name and a
 * {@link FormatFunction} to convert the input records, for example:
 *
 * <pre>{@code
 * PCollection<MyType> data = ...;
 * FormatFunction<MyType> fn = ...;
 *
 * data.apply("write",
 *     KuduIO.write()
 *         .withMasterAddresses("kudu1:8051,kudu2:8051,kudu3:8051")
 *         .withTable("table")
 *         .withFormatFn(fn));
 * }</pre>
 *
 * <h3>Experimental</h3>
 *
 * {@code KuduIO} does not support authentication in this release.
 */
@Experimental(Kind.SOURCE_SINK)
public class KuduIO {
  private static final Logger LOG = LoggerFactory.getLogger(KuduIO.class);

  private KuduIO() {}

  public static <T> Read<T> read() {
    return new AutoValue_KuduIO_Read.Builder<T>().setKuduService(new KuduServiceImpl<>()).build();
  }

  public static <T> Write<T> write() {
    return new AutoValue_KuduIO_Write.Builder<T>().setKuduService(new KuduServiceImpl<>()).build();
  }

  /**
   * An interface used by the KuduIO Write to convert an input record into an Operation to apply as
   * a mutation in Kudu.
   */
  @FunctionalInterface
  public interface FormatFunction<T> extends SerializableFunction<TableAndRecord<T>, Operation> {}

  /** Implementation of {@link KuduIO#read()}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable List<String> getMasterAddresses();

    abstract @Nullable String getTable();

    abstract @Nullable Integer getBatchSize();

    abstract @Nullable List<String> getProjectedColumns();

    abstract @Nullable List<Common.ColumnPredicatePB> getSerializablePredicates();

    abstract @Nullable Boolean getFaultTolerent();

    abstract @Nullable SerializableFunction<RowResult, T> getParseFn();

    abstract @Nullable Coder<T> getCoder();

    abstract @Nullable KuduService<T> getKuduService();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setMasterAddresses(List<String> masterAddresses);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setBatchSize(Integer batchSize);

      abstract Builder<T> setProjectedColumns(List<String> projectedColumns);

      abstract Builder<T> setSerializablePredicates(
          List<Common.ColumnPredicatePB> serializablePredicates);

      abstract Builder<T> setFaultTolerent(Boolean faultTolerent);

      abstract Builder<T> setParseFn(SerializableFunction<RowResult, T> parseFn);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setKuduService(KuduService<T> kuduService);

      abstract Read<T> build();
    }

    @VisibleForTesting
    Coder<T> inferCoder(CoderRegistry coderRegistry) {
      try {
        return getCoder() != null
            ? getCoder()
            : coderRegistry.getCoder(TypeDescriptors.outputOf(getParseFn()));
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(
            "Unable to infer coder for output of parseFn ("
                + TypeDescriptors.outputOf(getParseFn())
                + "). Specify it explicitly using withCoder().",
            e);
      }
    }

    /** Reads from the Kudu cluster on the specified master addresses. */
    public Read<T> withMasterAddresses(String masterAddresses) {
      checkArgument(masterAddresses != null, "masterAddresses cannot be null or empty");
      return builder().setMasterAddresses(Splitter.on(",").splitToList(masterAddresses)).build();
    }

    /** Reads from the specified table. */
    public Read<T> withTable(String table) {
      checkArgument(table != null, "table cannot be null");
      return builder().setTable(table).build();
    }

    /** Provides the function to parse a row from Kudu into the typed object. */
    public Read<T> withParseFn(SerializableFunction<RowResult, T> parseFn) {
      checkArgument(parseFn != null, "parseFn cannot be null");
      return builder().setParseFn(parseFn).build();
    }

    /** Filters the rows read from Kudu using the given predicates. */
    public Read<T> withPredicates(List<KuduPredicate> predicates) {
      checkArgument(predicates != null, "predicates cannot be null");
      // reuse the kudu protobuf serialization mechanism
      List<Common.ColumnPredicatePB> serializablePredicates =
          predicates.stream().map(KuduPredicate::toPB).collect(Collectors.toList());
      return builder().setSerializablePredicates(serializablePredicates).build();
    }

    /** Filters the columns read from the table to include only those specified. */
    public Read<T> withProjectedColumns(List<String> projectedColumns) {
      checkArgument(projectedColumns != null, "projectedColumns cannot be null");
      return builder().setProjectedColumns(projectedColumns).build();
    }

    /** Reads from the table in batches of the specified size. */
    public Read<T> withBatchSize(int batchSize) {
      checkArgument(batchSize >= 0, "batchSize must not be negative");
      return builder().setBatchSize(batchSize).build();
    }

    /**
     * Instructs the read scan to resume a scan on another tablet server if the current server fails
     * and faultTolerant is set to true.
     */
    public Read<T> withFaultTolerent(boolean faultTolerent) {
      return builder().setFaultTolerent(faultTolerent).build();
    }

    /**
     * Sets a {@link Coder} for the result of the parse function. This may be required if a coder
     * can not be inferred automatically.
     */
    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder cannot be null");
      return builder().setCoder(coder).build();
    }

    /** Specify an instance of {@link KuduService} used to connect and read from Kudu. */
    @VisibleForTesting
    Read<T> withKuduService(KuduService<T> kuduService) {
      checkArgument(kuduService != null, "kuduService cannot be null");
      return builder().setKuduService(kuduService).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      Pipeline p = input.getPipeline();
      final Coder<T> coder = inferCoder(p.getCoderRegistry());
      return input.apply(org.apache.beam.sdk.io.Read.from(new KuduSource<>(this, coder, null)));
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(
          getMasterAddresses() != null,
          "KuduIO.read() requires a list of master addresses to be set via withMasterAddresses(masterAddresses)");
      checkState(
          getTable() != null,
          "KuduIO.read() requires a table name to be set via withTableName(tableName)");
      checkState(
          getParseFn() != null,
          "KuduIO.read() requires a parse function to be set via withParseFn(parseFn)");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("masterAddresses", getMasterAddresses().toString()));
      builder.add(DisplayData.item("table", getTable()));
    }
  }

  static class KuduSource<T> extends BoundedSource {
    final Read<T> spec;
    private final Coder<T> coder;
    byte @Nullable [] serializedToken; // only during a split

    KuduSource(Read spec, Coder<T> coder, byte[] serializedToken) {
      this.spec = spec;
      this.coder = coder;
      this.serializedToken = serializedToken;
    }

    // A Kudu source can be split once only providing a source per tablet
    @Override
    public List<BoundedSource<T>> split(long desiredBundleSizeBytes, PipelineOptions options)
        throws KuduException {
      if (serializedToken != null) {
        return Collections.singletonList(this); // we are already a split

      } else {
        Stream<BoundedSource<T>> sources =
            spec.getKuduService().createTabletScanners(spec).stream()
                .map(s -> new KuduIO.KuduSource<T>(spec, spec.getCoder(), s));
        return sources.collect(Collectors.toList());
      }
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return 0; // Kudu does not expose tablet sizes
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions options) {
      return spec.getKuduService().createReader(this);
    }

    @Override
    public Coder<T> getOutputCoder() {
      return coder;
    }
  }

  /**
   * A {@link PTransform} that writes to Kudu. See the class-level Javadoc on {@link KuduIO} for
   * more information.
   *
   * @see KuduIO
   */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    abstract @Nullable List<String> masterAddresses();

    abstract @Nullable String table();

    abstract @Nullable FormatFunction<T> formatFn();

    abstract @Nullable KuduService<T> kuduService();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setMasterAddresses(List<String> masterAddresses);

      abstract Builder<T> setTable(String table);

      abstract Builder<T> setFormatFn(FormatFunction<T> formatFn);

      abstract Builder<T> setKuduService(KuduService<T> kuduService);

      abstract Write<T> build();
    }

    /** Writes to the Kudu cluster on the specified master addresses. */
    public Write withMasterAddresses(String masterAddresses) {
      checkArgument(masterAddresses != null, "masterAddresses cannot be null or empty");
      return builder().setMasterAddresses(Splitter.on(",").splitToList(masterAddresses)).build();
    }

    /** Writes to the specified table. */
    public Write withTable(String table) {
      checkArgument(table != null, "table cannot be null");
      return builder().setTable(table).build();
    }

    /** Writes using the given function to create the mutation operations from the input. */
    public Write withFormatFn(FormatFunction<T> formatFn) {
      checkArgument(formatFn != null, "formatFn cannot be null");
      return builder().setFormatFn(formatFn).build();
    }

    /** Specify the {@link KuduService} used to connect and write into the Kudu table. */
    @VisibleForTesting
    Write<T> withKuduService(KuduService<T> kuduService) {
      checkArgument(kuduService != null, "kuduService cannot be null");
      return builder().setKuduService(kuduService).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(
          masterAddresses() != null,
          "KuduIO.write() requires a list of master addresses to be set via withMasterAddresses(masterAddresses)");
      checkState(
          table() != null, "KuduIO.write() requires a table name to be set via withTable(table)");
      checkState(
          formatFn() != null,
          "KuduIO.write() requires a format function to be set via withFormatFn(formatFn)");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("masterAddresses", masterAddresses().toString()));
      builder.add(DisplayData.item("tableName", table()));
      builder.add(DisplayData.item("formatFn", formatFn().getClass().getCanonicalName()));
    }

    private class WriteFn extends DoFn<T, Void> {
      private final Write<T> spec;
      private KuduService.Writer writer;

      WriteFn(Write<T> spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws KuduException {
        writer = spec.kuduService().createWriter(spec);
      }

      @StartBundle
      public void startBundle(StartBundleContext context) throws KuduException {
        writer.openSession();
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws KuduException {
        writer.write(c.element());
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        writer.closeSession();
      }

      @Teardown
      public void teardown() throws Exception {
        writer.close();
        writer = null;
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.add(DisplayData.item("masterAddresses", spec.masterAddresses().toString()));
        builder.add(DisplayData.item("table", spec.table()));
      }
    }
  }
}
