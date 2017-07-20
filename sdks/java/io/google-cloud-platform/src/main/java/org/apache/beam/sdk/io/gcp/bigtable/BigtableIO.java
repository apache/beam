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
package org.apache.beam.sdk.io.gcp.bigtable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.CredentialOptions.CredentialType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.io.range.ByteKeyRangeTracker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform Transforms} for reading from and writing to Google Cloud Bigtable.
 *
 * <p>For more information about Cloud Bigtable, see the online documentation at
 * <a href="https://cloud.google.com/bigtable/">Google Cloud Bigtable</a>.
 *
 * <h3>Reading from Cloud Bigtable</h3>
 *
 * <p>The Bigtable source returns a set of rows from a single table, returning a
 * {@code PCollection<Row>}.
 *
 * <p>To configure a Cloud Bigtable source, you must supply a table id and a {@link BigtableOptions}
 * or builder configured with the project and other information necessary to identify the
 * Bigtable instance. By default, {@link BigtableIO.Read} will read all rows in the table. The row
 * range to be read can optionally be restricted using {@link BigtableIO.Read#withKeyRange}, and
 * a {@link RowFilter} can be specified using {@link BigtableIO.Read#withRowFilter}. For example:
 *
 * <pre>{@code
 * BigtableOptions.Builder optionsBuilder =
 *     new BigtableOptions.Builder()
 *         .setProjectId("project")
 *         .setInstanceId("instance");
 *
 * Pipeline p = ...;
 *
 * // Scan the entire table.
 * p.apply("read",
 *     BigtableIO.read()
 *         .withBigtableOptions(optionsBuilder)
 *         .withTableId("table"));
 *
 * // Scan a prefix of the table.
 * ByteKeyRange keyRange = ...;
 * p.apply("read",
 *     BigtableIO.read()
 *         .withBigtableOptions(optionsBuilder)
 *         .withTableId("table")
 *         .withKeyRange(keyRange));
 *
 * // Scan a subset of rows that match the specified row filter.
 * p.apply("filtered read",
 *     BigtableIO.read()
 *         .withBigtableOptions(optionsBuilder)
 *         .withTableId("table")
 *         .withRowFilter(filter));
 * }</pre>
 *
 * <h3>Writing to Cloud Bigtable</h3>
 *
 * <p>The Bigtable sink executes a set of row mutations on a single table. It takes as input a
 * {@link PCollection PCollection&lt;KV&lt;ByteString, Iterable&lt;Mutation&gt;&gt;&gt;}, where the
 * {@link ByteString} is the key of the row being mutated, and each {@link Mutation} represents an
 * idempotent transformation to that row.
 *
 * <p>To configure a Cloud Bigtable sink, you must supply a table id and a {@link BigtableOptions}
 * or builder configured with the project and other information necessary to identify the
 * Bigtable instance, for example:
 *
 * <pre>{@code
 * BigtableOptions.Builder optionsBuilder =
 *     new BigtableOptions.Builder()
 *         .setProjectId("project")
 *         .setInstanceId("instance");
 *
 * PCollection<KV<ByteString, Iterable<Mutation>>> data = ...;
 *
 * data.apply("write",
 *     BigtableIO.write()
 *         .withBigtableOptions(optionsBuilder)
 *         .withTableId("table"));
 * }</pre>
 *
 * <h3>Using local emulator</h3>
 *
 * <p>In order to use local emulator for Bigtable you should use:
 *
 * <pre>{@code
 * BigtableOptions.Builder optionsBuilder =
 *     new BigtableOptions.Builder()
 *         .setProjectId("project")
 *         .setInstanceId("instance")
 *         .setUsePlaintextNegotiation(true)
 *         .setCredentialOptions(CredentialOptions.nullCredential())
 *         .setDataHost("127.0.0.1") // network interface where Bigtable emulator is bound
 *         .setInstanceAdminHost("127.0.0.1")
 *         .setTableAdminHost("127.0.0.1")
 *         .setPort(LOCAL_EMULATOR_PORT))
 *
 * PCollection<KV<ByteString, Iterable<Mutation>>> data = ...;
 *
 * data.apply("write",
 *     BigtableIO.write()
 *         .withBigtableOptions(optionsBuilder)
 *         .withTableId("table");
 * }</pre>
 *
 * <h3>Experimental</h3>
 *
 * <p>This connector for Cloud Bigtable is considered experimental and may break or receive
 * backwards-incompatible changes in future versions of the Apache Beam SDK. Cloud Bigtable is
 * in Beta, and thus it may introduce breaking changes in future revisions of its service or APIs.
 *
 * <h3>Permissions</h3>
 *
 * <p>Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * pipeline. Please refer to the documentation of corresponding
 * {@link PipelineRunner PipelineRunners} for more details.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class BigtableIO {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableIO.class);

  /**
   * Creates an uninitialized {@link BigtableIO.Read}. Before use, the {@code Read} must be
   * initialized with a
   * {@link BigtableIO.Read#withBigtableOptions(BigtableOptions) BigtableOptions} that specifies
   * the source Cloud Bigtable instance, and a {@link BigtableIO.Read#withTableId tableId} that
   * specifies which table to read. A {@link RowFilter} may also optionally be specified using
   * {@link BigtableIO.Read#withRowFilter}.
   */
  @Experimental
  public static Read read() {
    return new AutoValue_BigtableIO_Read.Builder().setKeyRange(ByteKeyRange.ALL_KEYS).setTableId("")
        .build();
  }

  /**
   * Creates an uninitialized {@link BigtableIO.Write}. Before use, the {@code Write} must be
   * initialized with a
   * {@link BigtableIO.Write#withBigtableOptions(BigtableOptions) BigtableOptions} that specifies
   * the destination Cloud Bigtable instance, and a {@link BigtableIO.Write#withTableId tableId}
   * that specifies which table to write.
   */
  @Experimental
  public static Write write() {
    return new AutoValue_BigtableIO_Write.Builder().setTableId("").build();
  }

  /**
   * A {@link PTransform} that reads from Google Cloud Bigtable. See the class-level Javadoc on
   * {@link BigtableIO} for more information.
   *
   * @see BigtableIO
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Row>> {

    @Nullable
    abstract RowFilter getRowFilter();

    /** Returns the range of keys that will be read from the table. */
    @Nullable
    public abstract ByteKeyRange getKeyRange();

    /** Returns the table being read from. */
    @Nullable
    public abstract String getTableId();

    @Nullable
    abstract BigtableService getBigtableService();

    /** Returns the Google Cloud Bigtable instance being read from, and other parameters. */
    @Nullable
    public abstract BigtableOptions getBigtableOptions();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setRowFilter(RowFilter filter);

      abstract Builder setKeyRange(ByteKeyRange keyRange);

      abstract Builder setTableId(String tableId);

      abstract Builder setBigtableOptions(BigtableOptions options);

      abstract Builder setBigtableService(BigtableService bigtableService);

      abstract Read build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the Cloud Bigtable instance
     * indicated by the given options, and using any other specified customizations.
     *
     * <p>Does not modify this object.
     */
    public Read withBigtableOptions(BigtableOptions options) {
      checkNotNull(options, "options");
      return withBigtableOptions(options.toBuilder());
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the Cloud Bigtable instance
     * indicated by the given options, and using any other specified customizations.
     *
     * <p>Clones the given {@link BigtableOptions} builder so that any further changes
     * will have no effect on the returned {@link BigtableIO.Read}.
     *
     * <p>Does not modify this object.
     */
    public Read withBigtableOptions(BigtableOptions.Builder optionsBuilder) {
      checkNotNull(optionsBuilder, "optionsBuilder");
      // TODO: is there a better way to clone a Builder? Want it to be immune from user changes.
      BigtableOptions options = optionsBuilder.build();

      BigtableOptions.Builder clonedBuilder = options.toBuilder()
          .setUseCachedDataPool(true);
      BigtableOptions optionsWithAgent =
          clonedBuilder.setUserAgent(getBeamSdkPartOfUserAgent()).build();

      return toBuilder().setBigtableOptions(optionsWithAgent).build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will filter the rows read from Cloud Bigtable
     * using the given row filter.
     *
     * <p>Does not modify this object.
     */
    public Read withRowFilter(RowFilter filter) {
      checkNotNull(filter, "filter");
      return toBuilder().setRowFilter(filter).build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read only rows in the specified range.
     *
     * <p>Does not modify this object.
     */
    public Read withKeyRange(ByteKeyRange keyRange) {
      checkNotNull(keyRange, "keyRange");
      return toBuilder().setKeyRange(keyRange).build();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the specified table.
     *
     * <p>Does not modify this object.
     */
    public Read withTableId(String tableId) {
      checkNotNull(tableId, "tableId");
      return toBuilder().setTableId(tableId).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      BigtableSource source =
          new BigtableSource(new SerializableFunction<PipelineOptions, BigtableService>() {
            @Override
            public BigtableService apply(PipelineOptions options) {
              return getBigtableService(options);
            }
          }, getTableId(), getRowFilter(), getKeyRange(), null);
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
    }

    @Override
    public void validate(PipelineOptions options) {
      checkArgument(getBigtableOptions() != null, "BigtableOptions not specified");
      checkArgument(getTableId() != null && !getTableId().isEmpty(), "Table ID not specified");
      try {
        checkArgument(
            getBigtableService(options).tableExists(getTableId()),
            "Table %s does not exist",
            getTableId());
      } catch (IOException e) {
        LOG.warn("Error checking whether table {} exists; proceeding.", getTableId(), e);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.add(DisplayData.item("tableId", getTableId())
        .withLabel("Table ID"));

      if (getBigtableOptions() != null) {
        builder.add(DisplayData.item("bigtableOptions", getBigtableOptions().toString())
          .withLabel("Bigtable Options"));
      }

      builder.addIfNotDefault(
          DisplayData.item("keyRange", getKeyRange().toString()), ByteKeyRange.ALL_KEYS.toString());

      if (getRowFilter() != null) {
        builder.add(DisplayData.item("rowFilter", getRowFilter().toString())
          .withLabel("Table Row Filter"));
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Read.class)
          .add("options", getBigtableOptions())
          .add("tableId", getTableId())
          .add("keyRange", getKeyRange())
          .add("filter", getRowFilter())
          .toString();
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read using the given Cloud Bigtable
     * service implementation.
     *
     * <p>This is used for testing.
     *
     * <p>Does not modify this object.
     */
    Read withBigtableService(BigtableService bigtableService) {
      checkNotNull(bigtableService, "bigtableService");
      return toBuilder().setBigtableService(bigtableService).build();
    }

    /**
     * Helper function that either returns the mock Bigtable service supplied by
     * {@link #withBigtableService} or creates and returns an implementation that talks to
     * {@code Cloud Bigtable}.
     *
     * <p>Also populate the credentials option from {@link GcpOptions#getGcpCredential()} if the
     * default credentials are being used on {@link BigtableOptions}.
     */
    @VisibleForTesting
    BigtableService getBigtableService(PipelineOptions pipelineOptions) {
      if (getBigtableService() != null) {
        return getBigtableService();
      }
      BigtableOptions.Builder clonedOptions = getBigtableOptions().toBuilder();
      if (getBigtableOptions().getCredentialOptions()
          .getCredentialType() == CredentialType.DefaultCredentials) {
        clonedOptions.setCredentialOptions(
            CredentialOptions.credential(
                pipelineOptions.as(GcpOptions.class).getGcpCredential()));
      }
      return new BigtableServiceImpl(clonedOptions.build());
    }
  }

  /**
   * A {@link PTransform} that writes to Google Cloud Bigtable. See the class-level Javadoc on
   * {@link BigtableIO} for more information.
   *
   * @see BigtableIO
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  @AutoValue
  public abstract static class Write
      extends PTransform<PCollection<KV<ByteString, Iterable<Mutation>>>, PDone> {

    /** Returns the table being written to. */
    @Nullable
    abstract String getTableId();

    @Nullable
    abstract BigtableService getBigtableService();

    /** Returns the Google Cloud Bigtable instance being written to, and other parameters. */
    @Nullable
    public abstract BigtableOptions getBigtableOptions();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setTableId(String tableId);

      abstract Builder setBigtableOptions(BigtableOptions options);

      abstract Builder setBigtableService(BigtableService bigtableService);

      abstract Write build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write to the Cloud Bigtable instance
     * indicated by the given options, and using any other specified customizations.
     *
     * <p>Does not modify this object.
     */
    public Write withBigtableOptions(BigtableOptions options) {
      return withBigtableOptions(options.toBuilder());
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write to the Cloud Bigtable instance
     * indicated by the given options, and using any other specified customizations.
     *
     * <p>Clones the given {@link BigtableOptions} builder so that any further changes
     * will have no effect on the returned {@link BigtableIO.Write}.
     *
     * <p>Does not modify this object.
     */
    public Write withBigtableOptions(BigtableOptions.Builder optionsBuilder) {
      checkNotNull(optionsBuilder, "optionsBuilder");
      // TODO: is there a better way to clone a Builder? Want it to be immune from user changes.
      BigtableOptions options = optionsBuilder.build();

      // Set useBulkApi to true for enabling bulk writes
      BigtableOptions.Builder clonedBuilder = options.toBuilder()
          .setBulkOptions(
              options.getBulkOptions().toBuilder()
                  .setUseBulkApi(true)
                  .build())
          .setUseCachedDataPool(true);
      BigtableOptions optionsWithAgent =
          clonedBuilder.setUserAgent(getBeamSdkPartOfUserAgent()).build();
      return toBuilder().setBigtableOptions(optionsWithAgent).build();
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write to the specified table.
     *
     * <p>Does not modify this object.
     */
    public Write withTableId(String tableId) {
      checkNotNull(tableId, "tableId");
      return toBuilder().setTableId(tableId).build();
    }

    @Override
    public PDone expand(PCollection<KV<ByteString, Iterable<Mutation>>> input) {
      input.apply(ParDo.of(new BigtableWriterFn(getTableId(),
          new SerializableFunction<PipelineOptions, BigtableService>() {
        @Override
        public BigtableService apply(PipelineOptions options) {
          return getBigtableService(options);
        }
      })));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions options) {
      checkArgument(getBigtableOptions() != null, "BigtableOptions not specified");
      checkArgument(getTableId() != null && !getTableId().isEmpty(), "Table ID not specified");
      try {
        checkArgument(
            getBigtableService(options).tableExists(getTableId()),
            "Table %s does not exist",
            getTableId());
      } catch (IOException e) {
        LOG.warn("Error checking whether table {} exists; proceeding.", getTableId(), e);
      }
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write using the given Cloud Bigtable
     * service implementation.
     *
     * <p>This is used for testing.
     *
     * <p>Does not modify this object.
     */
    Write withBigtableService(BigtableService bigtableService) {
      checkNotNull(bigtableService, "bigtableService");
      return toBuilder().setBigtableService(bigtableService).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.add(DisplayData.item("tableId", getTableId())
        .withLabel("Table ID"));

      if (getBigtableOptions() != null) {
        builder.add(DisplayData.item("bigtableOptions", getBigtableOptions().toString())
          .withLabel("Bigtable Options"));
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Write.class)
          .add("options", getBigtableOptions())
          .add("tableId", getTableId())
          .toString();
    }

    /**
     * Helper function that either returns the mock Bigtable service supplied by
     * {@link #withBigtableService} or creates and returns an implementation that talks to
     * {@code Cloud Bigtable}.
     *
     * <p>Also populate the credentials option from {@link GcpOptions#getGcpCredential()} if the
     * default credentials are being used on {@link BigtableOptions}.
     */
    @VisibleForTesting
    BigtableService getBigtableService(PipelineOptions pipelineOptions) {
      if (getBigtableService() != null) {
        return getBigtableService();
      }
      BigtableOptions.Builder clonedOptions = getBigtableOptions().toBuilder();
      if (getBigtableOptions().getCredentialOptions()
          .getCredentialType() == CredentialType.DefaultCredentials) {
        clonedOptions.setCredentialOptions(
            CredentialOptions.credential(
                pipelineOptions.as(GcpOptions.class).getGcpCredential()));
      }
      return new BigtableServiceImpl(clonedOptions.build());
    }

    private class BigtableWriterFn extends DoFn<KV<ByteString, Iterable<Mutation>>, Void> {

      public BigtableWriterFn(String tableId,
          SerializableFunction<PipelineOptions, BigtableService> bigtableServiceFactory) {
        this.tableId = checkNotNull(tableId, "tableId");
        this.bigtableServiceFactory =
            checkNotNull(bigtableServiceFactory, "bigtableServiceFactory");
        this.failures = new ConcurrentLinkedQueue<>();
      }

      @StartBundle
      public void startBundle(StartBundleContext c) throws IOException {
        if (bigtableWriter == null) {
          bigtableWriter = bigtableServiceFactory.apply(
              c.getPipelineOptions()).openForWriting(tableId);
        }
        recordsWritten = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        checkForFailures();
        Futures.addCallback(
            bigtableWriter.writeRecord(c.element()), new WriteExceptionCallback(c.element()));
        ++recordsWritten;
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        bigtableWriter.flush();
        checkForFailures();
        LOG.info("Wrote {} records", recordsWritten);
      }

      @Teardown
      public void tearDown() throws Exception {
        bigtableWriter.close();
        bigtableWriter = null;
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.delegate(Write.this);
      }

      ///////////////////////////////////////////////////////////////////////////////
      private final String tableId;
      private final SerializableFunction<PipelineOptions, BigtableService> bigtableServiceFactory;
      private BigtableService.Writer bigtableWriter;
      private long recordsWritten;
      private final ConcurrentLinkedQueue<BigtableWriteException> failures;

      /**
       * If any write has asynchronously failed, fail the bundle with a useful error.
       */
      private void checkForFailures() throws IOException {
        // Note that this function is never called by multiple threads and is the only place that
        // we remove from failures, so this code is safe.
        if (failures.isEmpty()) {
          return;
        }

        StringBuilder logEntry = new StringBuilder();
        int i = 0;
        for (; i < 10 && !failures.isEmpty(); ++i) {
          BigtableWriteException exc = failures.remove();
          logEntry.append("\n").append(exc.getMessage());
          if (exc.getCause() != null) {
            logEntry.append(": ").append(exc.getCause().getMessage());
          }
        }
        String message =
            String.format(
                "At least %d errors occurred writing to Bigtable. First %d errors: %s",
                i + failures.size(),
                i,
                logEntry.toString());
        LOG.error(message);
        throw new IOException(message);
      }

      private class WriteExceptionCallback implements FutureCallback<MutateRowResponse> {
        private final KV<ByteString, Iterable<Mutation>> value;

        public WriteExceptionCallback(KV<ByteString, Iterable<Mutation>> value) {
          this.value = value;
        }

        @Override
        public void onFailure(Throwable cause) {
          failures.add(new BigtableWriteException(value, cause));
        }

        @Override
        public void onSuccess(MutateRowResponse produced) {}
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  /** Disallow construction of utility class. */
  private BigtableIO() {}

  private static ByteKey makeByteKey(ByteString key) {
    return ByteKey.copyFrom(key.asReadOnlyByteBuffer());
  }

  static class BigtableSource extends BoundedSource<Row> {
    public BigtableSource(
        SerializableFunction<PipelineOptions, BigtableService> serviceFactory,
        String tableId,
        @Nullable RowFilter filter,
        ByteKeyRange range,
        @Nullable Long estimatedSizeBytes) {
      this.serviceFactory = serviceFactory;
      this.tableId = tableId;
      this.filter = filter;
      this.range = range;
      this.estimatedSizeBytes = estimatedSizeBytes;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(BigtableSource.class)
          .add("tableId", tableId)
          .add("filter", filter)
          .add("range", range)
          .add("estimatedSizeBytes", estimatedSizeBytes)
          .toString();
    }

    ////// Private state and internal implementation details //////
    private final SerializableFunction<PipelineOptions, BigtableService> serviceFactory;
    private final String tableId;
    @Nullable private final RowFilter filter;
    private final ByteKeyRange range;
    @Nullable private Long estimatedSizeBytes;
    @Nullable private transient List<SampleRowKeysResponse> sampleRowKeys;

    protected BigtableSource withStartKey(ByteKey startKey) {
      checkNotNull(startKey, "startKey");
      return new BigtableSource(
          serviceFactory, tableId, filter, range.withStartKey(startKey), estimatedSizeBytes);
    }

    protected BigtableSource withEndKey(ByteKey endKey) {
      checkNotNull(endKey, "endKey");
      return new BigtableSource(
          serviceFactory, tableId, filter, range.withEndKey(endKey), estimatedSizeBytes);
    }

    protected BigtableSource withEstimatedSizeBytes(Long estimatedSizeBytes) {
      checkNotNull(estimatedSizeBytes, "estimatedSizeBytes");
      return new BigtableSource(serviceFactory, tableId, filter, range, estimatedSizeBytes);
    }

    /**
     * Makes an API call to the Cloud Bigtable service that gives information about tablet key
     * boundaries and estimated sizes. We can use these samples to ensure that splits are on
     * different tablets, and possibly generate sub-splits within tablets.
     */
    private List<SampleRowKeysResponse> getSampleRowKeys(PipelineOptions pipelineOptions)
        throws IOException {
      return serviceFactory.apply(pipelineOptions).getSampleRowKeys(this);
    }

    @Override
    public List<BigtableSource> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      // Update the desiredBundleSizeBytes in order to limit the
      // number of splits to maximumNumberOfSplits.
      long maximumNumberOfSplits = 4000;
      long sizeEstimate = getEstimatedSizeBytes(options);
      desiredBundleSizeBytes =
          Math.max(sizeEstimate / maximumNumberOfSplits, desiredBundleSizeBytes);

      // Delegate to testable helper.
      return splitBasedOnSamples(desiredBundleSizeBytes, getSampleRowKeys(options));
    }

    /** Helper that splits this source into bundles based on Cloud Bigtable sampled row keys. */
    private List<BigtableSource> splitBasedOnSamples(
        long desiredBundleSizeBytes, List<SampleRowKeysResponse> sampleRowKeys) {
      // There are no regions, or no samples available. Just scan the entire range.
      if (sampleRowKeys.isEmpty()) {
        LOG.info("Not splitting source {} because no sample row keys are available.", this);
        return Collections.singletonList(this);
      }

      LOG.info(
          "About to split into bundles of size {} with sampleRowKeys length {} first element {}",
          desiredBundleSizeBytes,
          sampleRowKeys.size(),
          sampleRowKeys.get(0));

      // Loop through all sampled responses and generate splits from the ones that overlap the
      // scan range. The main complication is that we must track the end range of the previous
      // sample to generate good ranges.
      ByteKey lastEndKey = ByteKey.EMPTY;
      long lastOffset = 0;
      ImmutableList.Builder<BigtableSource> splits = ImmutableList.builder();
      for (SampleRowKeysResponse response : sampleRowKeys) {
        ByteKey responseEndKey = makeByteKey(response.getRowKey());
        long responseOffset = response.getOffsetBytes();
        checkState(
            responseOffset >= lastOffset,
            "Expected response byte offset %s to come after the last offset %s",
            responseOffset,
            lastOffset);

        if (!range.overlaps(ByteKeyRange.of(lastEndKey, responseEndKey))) {
          // This region does not overlap the scan, so skip it.
          lastOffset = responseOffset;
          lastEndKey = responseEndKey;
          continue;
        }

        // Calculate the beginning of the split as the larger of startKey and the end of the last
        // split. Unspecified start is smallest key so is correctly treated as earliest key.
        ByteKey splitStartKey = lastEndKey;
        if (splitStartKey.compareTo(range.getStartKey()) < 0) {
          splitStartKey = range.getStartKey();
        }

        // Calculate the end of the split as the smaller of endKey and the end of this sample. Note
        // that range.containsKey handles the case when range.getEndKey() is empty.
        ByteKey splitEndKey = responseEndKey;
        if (!range.containsKey(splitEndKey)) {
          splitEndKey = range.getEndKey();
        }

        // We know this region overlaps the desired key range, and we know a rough estimate of its
        // size. Split the key range into bundle-sized chunks and then add them all as splits.
        long sampleSizeBytes = responseOffset - lastOffset;
        List<BigtableSource> subSplits =
            splitKeyRangeIntoBundleSizedSubranges(
                sampleSizeBytes,
                desiredBundleSizeBytes,
                ByteKeyRange.of(splitStartKey, splitEndKey));
        splits.addAll(subSplits);

        // Move to the next region.
        lastEndKey = responseEndKey;
        lastOffset = responseOffset;
      }

      // We must add one more region after the end of the samples if both these conditions hold:
      //  1. we did not scan to the end yet (lastEndKey is concrete, not 0-length).
      //  2. we want to scan to the end (endKey is empty) or farther (lastEndKey < endKey).
      if (!lastEndKey.isEmpty()
          && (range.getEndKey().isEmpty() || lastEndKey.compareTo(range.getEndKey()) < 0)) {
        splits.add(this.withStartKey(lastEndKey).withEndKey(range.getEndKey()));
      }

      List<BigtableSource> ret = splits.build();
      LOG.info("Generated {} splits. First split: {}", ret.size(), ret.get(0));
      return ret;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      // Delegate to testable helper.
      if (estimatedSizeBytes == null) {
        estimatedSizeBytes = getEstimatedSizeBytesBasedOnSamples(getSampleRowKeys(options));
      }
      return estimatedSizeBytes;
    }

    /**
     * Computes the estimated size in bytes based on the total size of all samples that overlap
     * the key range this source will scan.
     */
    private long getEstimatedSizeBytesBasedOnSamples(List<SampleRowKeysResponse> samples) {
      long estimatedSizeBytes = 0;
      long lastOffset = 0;
      ByteKey currentStartKey = ByteKey.EMPTY;
      // Compute the total estimated size as the size of each sample that overlaps the scan range.
      // TODO: In future, Bigtable service may provide finer grained APIs, e.g., to sample given a
      // filter or to sample on a given key range.
      for (SampleRowKeysResponse response : samples) {
        ByteKey currentEndKey = makeByteKey(response.getRowKey());
        long currentOffset = response.getOffsetBytes();
        if (!currentStartKey.isEmpty() && currentStartKey.equals(currentEndKey)) {
          // Skip an empty region.
          lastOffset = currentOffset;
          continue;
        } else if (range.overlaps(ByteKeyRange.of(currentStartKey, currentEndKey))) {
          estimatedSizeBytes += currentOffset - lastOffset;
        }
        currentStartKey = currentEndKey;
        lastOffset = currentOffset;
      }
      return estimatedSizeBytes;
    }

    @Override
    public BoundedReader<Row> createReader(PipelineOptions options) throws IOException {
      return new BigtableReader(this, serviceFactory.apply(options));
    }

    @Override
    public void validate() {
      checkArgument(!tableId.isEmpty(), "tableId cannot be empty");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.add(DisplayData.item("tableId", tableId)
          .withLabel("Table ID"));

      if (filter != null) {
        builder.add(DisplayData.item("rowFilter", filter.toString())
            .withLabel("Table Row Filter"));
      }
    }

    @Override
    public Coder<Row> getDefaultOutputCoder() {
      return ProtoCoder.of(Row.class);
    }

    /** Helper that splits the specified range in this source into bundles. */
    private List<BigtableSource> splitKeyRangeIntoBundleSizedSubranges(
        long sampleSizeBytes, long desiredBundleSizeBytes, ByteKeyRange range) {
      // Catch the trivial cases. Split is small enough already, or this is the last region.
      LOG.debug(
          "Subsplit for sampleSizeBytes {} and desiredBundleSizeBytes {}",
          sampleSizeBytes,
          desiredBundleSizeBytes);
      if (sampleSizeBytes <= desiredBundleSizeBytes) {
        return Collections.singletonList(
            this.withStartKey(range.getStartKey()).withEndKey(range.getEndKey()));
      }

      checkArgument(
          sampleSizeBytes > 0, "Sample size %s bytes must be greater than 0.", sampleSizeBytes);
      checkArgument(
          desiredBundleSizeBytes > 0,
          "Desired bundle size %s bytes must be greater than 0.",
          desiredBundleSizeBytes);

      int splitCount = (int) Math.ceil(((double) sampleSizeBytes) / (desiredBundleSizeBytes));
      List<ByteKey> splitKeys = range.split(splitCount);
      ImmutableList.Builder<BigtableSource> splits = ImmutableList.builder();
      Iterator<ByteKey> keys = splitKeys.iterator();
      ByteKey prev = keys.next();
      while (keys.hasNext()) {
        ByteKey next = keys.next();
        splits.add(
            this
                .withStartKey(prev)
                .withEndKey(next)
                .withEstimatedSizeBytes(sampleSizeBytes / splitCount));
        prev = next;
      }
      return splits.build();
    }

    public ByteKeyRange getRange() {
      return range;
    }

    public RowFilter getRowFilter() {
      return filter;
    }

    public String getTableId() {
      return tableId;
    }
  }

  private static class BigtableReader extends BoundedReader<Row> {
    // Thread-safety: source is protected via synchronization and is only accessed or modified
    // inside a synchronized block (or constructor, which is the same).
    private BigtableSource source;
    private BigtableService service;
    private BigtableService.Reader reader;
    private final ByteKeyRangeTracker rangeTracker;
    private long recordsReturned;

    public BigtableReader(BigtableSource source, BigtableService service) {
      this.source = source;
      this.service = service;
      rangeTracker = ByteKeyRangeTracker.of(source.getRange());
    }

    @Override
    public boolean start() throws IOException {
      reader = service.createReader(getCurrentSource());
      boolean hasRecord =
          reader.start()
              && rangeTracker.tryReturnRecordAt(true, makeByteKey(reader.getCurrentRow().getKey()))
              || rangeTracker.markDone();
      if (hasRecord) {
        ++recordsReturned;
      }
      return hasRecord;
    }

    @Override
    public synchronized BigtableSource getCurrentSource() {
      return source;
    }

    @Override
    public boolean advance() throws IOException {
      boolean hasRecord =
          reader.advance()
              && rangeTracker.tryReturnRecordAt(true, makeByteKey(reader.getCurrentRow().getKey()))
              || rangeTracker.markDone();
      if (hasRecord) {
        ++recordsReturned;
      }
      return hasRecord;
    }

    @Override
    public Row getCurrent() throws NoSuchElementException {
      return reader.getCurrentRow();
    }

    @Override
    public void close() throws IOException {
      LOG.info("Closing reader after reading {} records.", recordsReturned);
      if (reader != null) {
        reader.close();
        reader = null;
      }
    }

    @Override
    public final Double getFractionConsumed() {
      return rangeTracker.getFractionConsumed();
    }

    @Override
    public final long getSplitPointsConsumed() {
      return rangeTracker.getSplitPointsConsumed();
    }

    @Override
    @Nullable
    public final synchronized BigtableSource splitAtFraction(double fraction) {
      ByteKey splitKey;
      try {
        splitKey = rangeTracker.getRange().interpolateKey(fraction);
      } catch (RuntimeException e) {
        LOG.info(
            "{}: Failed to interpolate key for fraction {}.", rangeTracker.getRange(), fraction, e);
        return null;
      }
      LOG.info(
          "Proposing to split {} at fraction {} (key {})", rangeTracker, fraction, splitKey);
      BigtableSource primary;
      BigtableSource residual;
      try {
         primary = source.withEndKey(splitKey);
         residual =  source.withStartKey(splitKey);
      } catch (RuntimeException e) {
        LOG.info(
            "{}: Interpolating for fraction {} yielded invalid split key {}.",
            rangeTracker.getRange(),
            fraction,
            splitKey,
            e);
        return null;
      }
      if (!rangeTracker.trySplitAtPosition(splitKey)) {
        return null;
      }
      this.source = primary;
      return residual;
    }
  }

  /**
   * An exception that puts information about the failed record being written in its message.
   */
  static class BigtableWriteException extends IOException {
    public BigtableWriteException(KV<ByteString, Iterable<Mutation>> record, Throwable cause) {
      super(
          String.format(
              "Error mutating row %s with mutations %s",
              record.getKey().toStringUtf8(),
              record.getValue()),
          cause);
    }
  }

  /**
   * A helper function to produce a Cloud Bigtable user agent string. This need only include
   * information about the Apache Beam SDK itself, because Bigtable will automatically append
   * other relevant system and Bigtable client-specific version information.
   *
   * @see com.google.cloud.bigtable.config.BigtableVersionInfo
   */
  private static String getBeamSdkPartOfUserAgent() {
    ReleaseInfo info = ReleaseInfo.getReleaseInfo();
    return
        String.format("%s/%s", info.getName(), info.getVersion())
            .replace(" ", "_");
  }
}
