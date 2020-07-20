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
package org.apache.beam.sdk.io.hbase;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.io.range.ByteKeyRangeTracker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bounded source and sink for HBase.
 *
 * <p>For more information, see the online documentation at <a
 * href="https://hbase.apache.org/">HBase</a>.
 *
 * <h3>Reading from HBase</h3>
 *
 * <p>The HBase source returns a set of rows from a single table, returning a {@code
 * PCollection<Result>}.
 *
 * <p>To configure a HBase source, you must supply a table id and a {@link Configuration} to
 * identify the HBase instance. By default, {@link HBaseIO.Read} will read all rows in the table.
 * The row range to be read can optionally be restricted using with a {@link Scan} object or using
 * the {@link HBaseIO.Read#withKeyRange}, and a {@link Filter} using {@link
 * HBaseIO.Read#withFilter}, for example:
 *
 * <pre>{@code
 * // Scan the entire table.
 * p.apply("read",
 *     HBaseIO.read()
 *         .withConfiguration(configuration)
 *         .withTableId("table"));
 *
 * // Filter data using a HBaseIO Scan
 * Scan scan = ...
 * p.apply("read",
 *     HBaseIO.read()
 *         .withConfiguration(configuration)
 *         .withTableId("table"))
 *         .withScan(scan));
 *
 * // Scan a prefix of the table.
 * ByteKeyRange keyRange = ...;
 * p.apply("read",
 *     HBaseIO.read()
 *         .withConfiguration(configuration)
 *         .withTableId("table")
 *         .withKeyRange(keyRange));
 *
 * // Scan a subset of rows that match the specified row filter.
 * p.apply("filtered read",
 *     HBaseIO.read()
 *         .withConfiguration(configuration)
 *         .withTableId("table")
 *         .withFilter(filter));
 * }</pre>
 *
 * <p>{@link HBaseIO#readAll()} allows to execute multiple {@link Scan}s to multiple {@link Table}s.
 * These queries are encapsulated via an initial {@link PCollection} of {@link Read}s and can be
 * used to create advanced compositional patterns like reading from a Source and then based on the
 * data create new HBase scans.
 *
 * <p><b>Note:</b> {@link HBaseIO.ReadAll} only works with <a
 * href="https://beam.apache.org/documentation/runners/capability-matrix/">runners that support
 * Splittable DoFn</a>.
 *
 * <pre>{@code
 * PCollection<Read> queries = ...;
 * queries.apply("readAll", HBaseIO.readAll().withConfiguration(configuration));
 * }</pre>
 *
 * <h3>Writing to HBase</h3>
 *
 * <p>The HBase sink executes a set of row mutations on a single table. It takes as input a {@link
 * PCollection PCollection&lt;Mutation&gt;}, where each {@link Mutation} represents an idempotent
 * transformation on a row.
 *
 * <p>To configure a HBase sink, you must supply a table id and a {@link Configuration} to identify
 * the HBase instance, for example:
 *
 * <pre>{@code
 * Configuration configuration = ...;
 * PCollection<Mutation> data = ...;
 *
 * data.apply("write",
 *     HBaseIO.write()
 *         .withConfiguration(configuration)
 *         .withTableId("table"));
 * }</pre>
 *
 * <h3>Experimental</h3>
 *
 * <p>The design of the API for HBaseIO is currently related to the BigtableIO one, it can evolve or
 * be different in some aspects, but the idea is that users can easily migrate from one to the other
 * .
 */
@Experimental(Kind.SOURCE_SINK)
public class HBaseIO {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseIO.class);

  /** Disallow construction of utility class. */
  private HBaseIO() {}

  /**
   * Creates an uninitialized {@link HBaseIO.Read}. Before use, the {@code Read} must be initialized
   * with a {@link HBaseIO.Read#withConfiguration(Configuration)} that specifies the HBase instance,
   * and a {@link HBaseIO.Read#withTableId tableId} that specifies which table to read. A {@link
   * Filter} may also optionally be specified using {@link HBaseIO.Read#withFilter}.
   */
  public static Read read() {
    return new Read(null, "", new Scan());
  }

  /**
   * A {@link PTransform} that reads from HBase. See the class-level Javadoc on {@link HBaseIO} for*
   * more information.
   *
   * @see HBaseIO
   */
  public static class Read extends PTransform<PBegin, PCollection<Result>> {
    /** Reads from the HBase instance indicated by the* given configuration. */
    public Read withConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "configuration cannot be null");
      return new Read(new Configuration(configuration), tableId, scan);
    }

    /** Reads from the specified table. */
    public Read withTableId(String tableId) {
      checkArgument(tableId != null, "tableId cannot be null");
      return new Read(configuration, tableId, scan);
    }

    /** Filters the rows read from HBase using the given* scan. */
    public Read withScan(Scan scan) {
      checkArgument(scan != null, "scan cannot be null");
      return new Read(configuration, tableId, scan);
    }

    /** Filters the rows read from HBase using the given* row filter. */
    public Read withFilter(Filter filter) {
      checkArgument(filter != null, "filter cannot be null");
      return withScan(scan.setFilter(filter));
    }

    /** Reads only rows in the specified range. */
    public Read withKeyRange(ByteKeyRange keyRange) {
      checkArgument(keyRange != null, "keyRange cannot be null");
      byte[] startRow = keyRange.getStartKey().getBytes();
      byte[] stopRow = keyRange.getEndKey().getBytes();
      return withScan(scan.setStartRow(startRow).setStopRow(stopRow));
    }

    /** Reads only rows in the specified range. */
    public Read withKeyRange(byte[] startRow, byte[] stopRow) {
      checkArgument(startRow != null, "startRow cannot be null");
      checkArgument(stopRow != null, "stopRow cannot be null");
      ByteKeyRange keyRange =
          ByteKeyRange.of(ByteKey.copyFrom(startRow), ByteKey.copyFrom(stopRow));
      return withKeyRange(keyRange);
    }

    private Read(Configuration configuration, String tableId, Scan scan) {
      this.configuration = configuration;
      this.tableId = tableId;
      this.scan = scan;
    }

    @Override
    public PCollection<Result> expand(PBegin input) {
      checkArgument(configuration != null, "withConfiguration() is required");
      checkArgument(!tableId.isEmpty(), "withTableId() is required");
      try (Connection connection = ConnectionFactory.createConnection(configuration)) {
        Admin admin = connection.getAdmin();
        checkArgument(
            admin.tableExists(TableName.valueOf(tableId)), "Table %s does not exist", tableId);
      } catch (IOException e) {
        LOG.warn("Error checking whether table {} exists; proceeding.", tableId, e);
      }

      return input.apply(
          org.apache.beam.sdk.io.Read.from(new HBaseSource(this, null /* estimatedSizeBytes */)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("configuration", configuration.toString()));
      builder.add(DisplayData.item("tableId", tableId));
      builder.addIfNotNull(DisplayData.item("scan", scan.toString()));
    }

    public Configuration getConfiguration() {
      return configuration;
    }

    public String getTableId() {
      return tableId;
    }

    public Scan getScan() {
      return scan;
    }

    /** Returns the range of keys that will be read from the table. */
    public ByteKeyRange getKeyRange() {
      byte[] startRow = scan.getStartRow();
      byte[] stopRow = scan.getStopRow();
      return ByteKeyRange.of(ByteKey.copyFrom(startRow), ByteKey.copyFrom(stopRow));
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Read read = (Read) o;
      return configuration.toString().equals(read.configuration.toString())
          && Objects.equals(tableId, read.tableId)
          && scan.toString().equals(read.scan.toString());
    }

    @Override
    public int hashCode() {
      return Objects.hash(configuration, tableId, scan);
    }

    /**
     * The writeReplace method allows the developer to provide a replacement object that will be
     * serialized instead of the original one. We use this to keep the enclosed class immutable. For
     * more details on the technique see <a
     * href="https://lingpipe-blog.com/2009/08/10/serializing-immutable-singletons-serialization-proxy/">this
     * article</a>.
     */
    private Object writeReplace() {
      return new SerializationProxy(this);
    }

    private static class SerializationProxy implements Serializable {
      public SerializationProxy() {}

      public SerializationProxy(Read read) {
        configuration = read.configuration;
        tableId = read.tableId;
        scan = read.scan;
      }

      private void writeObject(ObjectOutputStream out) throws IOException {
        SerializableCoder.of(SerializableConfiguration.class)
            .encode(new SerializableConfiguration(this.configuration), out);
        StringUtf8Coder.of().encode(this.tableId, out);
        ProtobufUtil.toScan(this.scan).writeDelimitedTo(out);
      }

      private void readObject(ObjectInputStream in) throws IOException {
        this.configuration = SerializableCoder.of(SerializableConfiguration.class).decode(in).get();
        this.tableId = StringUtf8Coder.of().decode(in);
        this.scan = ProtobufUtil.toScan(ClientProtos.Scan.parseDelimitedFrom(in));
      }

      Object readResolve() {
        return HBaseIO.read().withConfiguration(configuration).withTableId(tableId).withScan(scan);
      }

      private Configuration configuration;
      private String tableId;
      private Scan scan;
    }

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Configuration configuration;

    private final String tableId;

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Scan scan;
  }

  /**
   * A {@link PTransform} that works like {@link #read}, but executes read operations coming from a
   * {@link PCollection} of {@link Read}.
   */
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public static ReadAll readAll() {
    return new ReadAll();
  }

  /** Implementation of {@link #readAll}. */
  public static class ReadAll extends PTransform<PCollection<Read>, PCollection<Result>> {
    private ReadAll() {}

    @Override
    public PCollection<Result> expand(PCollection<Read> input) {
      return input.apply(ParDo.of(new HBaseReadSplittableDoFn()));
    }
  }

  static class HBaseSource extends BoundedSource<Result> {
    private final Read read;
    private @Nullable Long estimatedSizeBytes;

    HBaseSource(Read read, @Nullable Long estimatedSizeBytes) {
      this.read = read;
      this.estimatedSizeBytes = estimatedSizeBytes;
    }

    HBaseSource withStartKey(ByteKey startKey) throws IOException {
      checkNotNull(startKey, "startKey");
      Read newRead =
          new Read(
              read.configuration,
              read.tableId,
              new Scan(read.scan).setStartRow(startKey.getBytes()));
      return new HBaseSource(newRead, estimatedSizeBytes);
    }

    HBaseSource withEndKey(ByteKey endKey) throws IOException {
      checkNotNull(endKey, "endKey");
      Read newRead =
          new Read(
              read.configuration, read.tableId, new Scan(read.scan).setStopRow(endKey.getBytes()));
      return new HBaseSource(newRead, estimatedSizeBytes);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
      if (estimatedSizeBytes == null) {
        try (Connection connection = ConnectionFactory.createConnection(read.configuration)) {
          estimatedSizeBytes =
              HBaseUtils.estimateSizeBytes(
                  connection, read.tableId, HBaseUtils.getByteKeyRange(read.scan));
        }
        LOG.debug(
            "Estimated size {} bytes for table {} and scan {}",
            estimatedSizeBytes,
            read.tableId,
            read.scan);
      }
      return estimatedSizeBytes;
    }

    @Override
    public List<? extends BoundedSource<Result>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      LOG.debug("desiredBundleSize {} bytes", desiredBundleSizeBytes);
      long estimatedSizeBytes = getEstimatedSizeBytes(options);
      int numSplits = 1;
      if (estimatedSizeBytes > 0 && desiredBundleSizeBytes > 0) {
        numSplits = (int) Math.ceil((double) estimatedSizeBytes / desiredBundleSizeBytes);
      }

      try (Connection connection = ConnectionFactory.createConnection(read.getConfiguration())) {
        List<HRegionLocation> regionLocations =
            HBaseUtils.getRegionLocations(
                connection, read.tableId, HBaseUtils.getByteKeyRange(read.scan));
        LOG.debug("Suggested {} source(s) based on size", numSplits);
        LOG.debug("Suggested {} source(s) based on number of regions", regionLocations.size());

        List<ByteKeyRange> ranges =
            HBaseUtils.getRanges(
                regionLocations, read.tableId, HBaseUtils.getByteKeyRange(read.scan));
        final int numSources = ranges.size();
        LOG.debug("Spliting into {} source(s)", numSources);
        if (numSources > 0) {
          List<HBaseSource> sources = new ArrayList<>(numSources);
          for (int i = 0; i < numSources; i++) {
            final ByteKeyRange range = ranges.get(i);
            LOG.debug("Range {}: {} - {}", i, range.getStartKey(), range.getEndKey());
            // We create a new copy of the scan to read from the new ranges
            sources.add(
                new HBaseSource(
                    new Read(
                        read.configuration,
                        read.tableId,
                        new Scan(read.scan)
                            .setStartRow(range.getStartKey().getBytes())
                            .setStopRow(range.getEndKey().getBytes())),
                    estimatedSizeBytes));
          }
          return sources;
        }
      }

      return Collections.singletonList(this);
    }

    @Override
    public BoundedReader<Result> createReader(PipelineOptions pipelineOptions) {
      return new HBaseReader(this);
    }

    @Override
    public void validate() {
      read.validate(null /* input */);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      read.populateDisplayData(builder);
    }

    @Override
    public Coder<Result> getOutputCoder() {
      return HBaseResultCoder.of();
    }
  }

  private static class HBaseReader extends BoundedSource.BoundedReader<Result> {
    private HBaseSource source;
    private Connection connection;
    private ResultScanner scanner;
    private Iterator<Result> iter;
    private Result current;
    private final ByteKeyRangeTracker rangeTracker;
    private long recordsReturned;

    HBaseReader(HBaseSource source) {
      this.source = source;
      Scan scan = source.read.scan;
      ByteKeyRange range =
          ByteKeyRange.of(
              ByteKey.copyFrom(scan.getStartRow()), ByteKey.copyFrom(scan.getStopRow()));
      rangeTracker = ByteKeyRangeTracker.of(range);
    }

    @Override
    public boolean start() throws IOException {
      HBaseSource source = getCurrentSource();
      Configuration configuration = source.read.configuration;
      String tableId = source.read.tableId;
      connection = ConnectionFactory.createConnection(configuration);
      TableName tableName = TableName.valueOf(tableId);
      Table table = connection.getTable(tableName);
      // [BEAM-2319] We have to clone the Scan because the underlying scanner may mutate it.
      Scan scanClone = new Scan(source.read.scan);
      scanner = table.getScanner(scanClone);
      iter = scanner.iterator();
      return advance();
    }

    @Override
    public Result getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public boolean advance() {
      if (!iter.hasNext()) {
        return rangeTracker.markDone();
      }
      final Result next = iter.next();
      boolean hasRecord =
          rangeTracker.tryReturnRecordAt(true, ByteKey.copyFrom(next.getRow()))
              || rangeTracker.markDone();
      if (hasRecord) {
        current = next;
        ++recordsReturned;
      }
      return hasRecord;
    }

    @Override
    public void close() throws IOException {
      LOG.debug("Closing reader after reading {} records.", recordsReturned);
      if (scanner != null) {
        scanner.close();
        scanner = null;
      }
      if (connection != null) {
        connection.close();
        connection = null;
      }
    }

    @Override
    public synchronized HBaseSource getCurrentSource() {
      return source;
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
    public final synchronized @Nullable HBaseSource splitAtFraction(double fraction) {
      ByteKey splitKey;
      try {
        splitKey = rangeTracker.getRange().interpolateKey(fraction);
      } catch (RuntimeException e) {
        LOG.info(
            "{}: Failed to interpolate key for fraction {}.", rangeTracker.getRange(), fraction, e);
        return null;
      }
      LOG.info("Proposing to split {} at fraction {} (key {})", rangeTracker, fraction, splitKey);
      HBaseSource primary;
      HBaseSource residual;
      try {
        primary = source.withEndKey(splitKey);
        residual = source.withStartKey(splitKey);
      } catch (Exception e) {
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
   * Creates an uninitialized {@link HBaseIO.Write}. Before use, the {@code Write} must be
   * initialized with a {@link HBaseIO.Write#withConfiguration(Configuration)} that specifies the
   * destination HBase instance, and a {@link HBaseIO.Write#withTableId tableId} that specifies
   * which table to write.
   */
  public static Write write() {
    return new Write(null /* Configuration */, "");
  }

  /**
   * A {@link PTransform} that writes to HBase. See the class-level Javadoc on {@link HBaseIO} for*
   * more information.
   *
   * @see HBaseIO
   */
  public static class Write extends PTransform<PCollection<Mutation>, PDone> {
    /** Writes to the HBase instance indicated by the* given Configuration. */
    public Write withConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "configuration cannot be null");
      return new Write(configuration, tableId);
    }

    /** Writes to the specified table. */
    public Write withTableId(String tableId) {
      checkArgument(tableId != null, "tableId cannot be null");
      return new Write(configuration, tableId);
    }

    private Write(Configuration configuration, String tableId) {
      this.configuration = configuration;
      this.tableId = tableId;
    }

    @Override
    public PDone expand(PCollection<Mutation> input) {
      checkArgument(configuration != null, "withConfiguration() is required");
      checkArgument(tableId != null && !tableId.isEmpty(), "withTableId() is required");
      try (Connection connection = ConnectionFactory.createConnection(configuration)) {
        Admin admin = connection.getAdmin();
        checkArgument(
            admin.tableExists(TableName.valueOf(tableId)), "Table %s does not exist", tableId);
      } catch (IOException e) {
        LOG.warn("Error checking whether table {} exists; proceeding.", tableId, e);
      }
      input.apply(ParDo.of(new HBaseWriterFn(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("configuration", configuration.toString()));
      builder.add(DisplayData.item("tableId", tableId));
    }

    public Configuration getConfiguration() {
      return configuration;
    }

    public String getTableId() {
      return tableId;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Write write = (Write) o;
      return configuration.toString().equals(write.configuration.toString())
          && Objects.equals(tableId, write.tableId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(configuration, tableId);
    }

    /**
     * The writeReplace method allows the developer to provide a replacement object that will be
     * serialized instead of the original one. We use this to keep the enclosed class immutable. For
     * more details on the technique see <a
     * href="https://lingpipe-blog.com/2009/08/10/serializing-immutable-singletons-serialization-proxy/">this
     * article</a>.
     */
    private Object writeReplace() {
      return new SerializationProxy(this);
    }

    private static class SerializationProxy implements Serializable {
      public SerializationProxy() {}

      public SerializationProxy(Write write) {
        configuration = write.configuration;
        tableId = write.tableId;
      }

      private void writeObject(ObjectOutputStream out) throws IOException {
        SerializableCoder.of(SerializableConfiguration.class)
            .encode(new SerializableConfiguration(this.configuration), out);
        StringUtf8Coder.of().encode(this.tableId, out);
      }

      private void readObject(ObjectInputStream in) throws IOException {
        this.configuration = SerializableCoder.of(SerializableConfiguration.class).decode(in).get();
        this.tableId = StringUtf8Coder.of().decode(in);
      }

      Object readResolve() {
        return HBaseIO.write().withConfiguration(configuration).withTableId(tableId);
      }

      private Configuration configuration;
      private String tableId;
    }

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final Configuration configuration;

    private final String tableId;

    private class HBaseWriterFn extends DoFn<Mutation, Void> {

      HBaseWriterFn(Write write) {
        checkNotNull(write.tableId, "tableId");
        checkNotNull(write.configuration, "configuration");
        this.write = write;
      }

      @Setup
      public void setup() throws Exception {
        connection = ConnectionFactory.createConnection(configuration);
      }

      @StartBundle
      public void startBundle(StartBundleContext c) throws IOException {
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableId));
        mutator = connection.getBufferedMutator(params);
        recordsWritten = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        mutator.mutate(c.element());
        ++recordsWritten;
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        mutator.flush();
        LOG.debug("Wrote {} records", recordsWritten);
      }

      @Teardown
      public void tearDown() throws Exception {
        if (mutator != null) {
          mutator.close();
          mutator = null;
        }
        if (connection != null) {
          connection.close();
          connection = null;
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.delegate(Write.this);
      }

      private final Write write;
      private long recordsWritten;

      private transient Connection connection;
      private transient BufferedMutator mutator;
    }
  }
}
