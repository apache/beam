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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
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
 * These queries are encapsulated via an initial {@link PCollection} of {@link HBaseQuery}s and can
 * be used to create advanced compositional patterns like reading from a Source and then based on
 * the data create new HBase scans.
 *
 * <p><b>Note:</b> {@link HBaseIO.ReadAll} only works with <a
 * href="https://beam.apache.org/documentation/runners/capability-matrix/">runners that support
 * Splittable DoFn</a>.
 *
 * <pre>{@code
 * PCollection<HBaseQuery> queries = ...;
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
    return new Read(null, "", new SerializableScan(new Scan()));
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
      checkArgument(configuration != null, "configuration can not be null");
      return new Read(new SerializableConfiguration(configuration), tableId, serializableScan);
    }

    /** Reads from the specified table. */
    public Read withTableId(String tableId) {
      checkArgument(tableId != null, "tableIdcan not be null");
      return new Read(serializableConfiguration, tableId, serializableScan);
    }

    /** Filters the rows read from HBase using the given* scan. */
    public Read withScan(Scan scan) {
      checkArgument(scan != null, "scancan not be null");
      return new Read(serializableConfiguration, tableId, new SerializableScan(scan));
    }

    /** Filters the rows read from HBase using the given* row filter. */
    public Read withFilter(Filter filter) {
      checkArgument(filter != null, "filtercan not be null");
      return withScan(serializableScan.get().setFilter(filter));
    }

    /** Reads only rows in the specified range. */
    public Read withKeyRange(ByteKeyRange keyRange) {
      checkArgument(keyRange != null, "keyRangecan not be null");
      byte[] startRow = keyRange.getStartKey().getBytes();
      byte[] stopRow = keyRange.getEndKey().getBytes();
      return withScan(serializableScan.get().setStartRow(startRow).setStopRow(stopRow));
    }

    /** Reads only rows in the specified range. */
    public Read withKeyRange(byte[] startRow, byte[] stopRow) {
      checkArgument(startRow != null, "startRowcan not be null");
      checkArgument(stopRow != null, "stopRowcan not be null");
      ByteKeyRange keyRange =
          ByteKeyRange.of(ByteKey.copyFrom(startRow), ByteKey.copyFrom(stopRow));
      return withKeyRange(keyRange);
    }

    private Read(
        SerializableConfiguration serializableConfiguration,
        String tableId,
        SerializableScan serializableScan) {
      this.serializableConfiguration = serializableConfiguration;
      this.tableId = tableId;
      this.serializableScan = serializableScan;
    }

    @Override
    public PCollection<Result> expand(PBegin input) {
      checkArgument(serializableConfiguration != null, "withConfiguration() is required");
      checkArgument(!tableId.isEmpty(), "withTableId() is required");
      try (Connection connection =
          ConnectionFactory.createConnection(serializableConfiguration.get())) {
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
      builder.add(DisplayData.item("configuration", serializableConfiguration.get().toString()));
      builder.add(DisplayData.item("tableId", tableId));
      builder.addIfNotNull(DisplayData.item("scan", serializableScan.get().toString()));
    }

    public Configuration getConfiguration() {
      return serializableConfiguration.get();
    }

    public String getTableId() {
      return tableId;
    }

    public Scan getScan() {
      return serializableScan.get();
    }

    /** Returns the range of keys that will be read from the table. */
    public ByteKeyRange getKeyRange() {
      byte[] startRow = serializableScan.get().getStartRow();
      byte[] stopRow = serializableScan.get().getStopRow();
      return ByteKeyRange.of(ByteKey.copyFrom(startRow), ByteKey.copyFrom(stopRow));
    }

    private final SerializableConfiguration serializableConfiguration;
    private final String tableId;
    private final SerializableScan serializableScan;
  }

  /**
   * A {@link PTransform} that works like {@link #read}, but executes read operations coming from a
   * {@link PCollection} of {@link HBaseQuery}.
   */
  @Experimental(Kind.SPLITTABLE_DO_FN)
  public static ReadAll readAll() {
    return new ReadAll(null);
  }

  /** Implementation of {@link #readAll}. */
  public static class ReadAll extends PTransform<PCollection<HBaseQuery>, PCollection<Result>> {

    private ReadAll(SerializableConfiguration serializableConfiguration) {
      this.serializableConfiguration = serializableConfiguration;
    }

    /** Reads from the HBase instance indicated by the* given configuration. */
    public ReadAll withConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return new ReadAll(new SerializableConfiguration(configuration));
    }

    @Override
    public PCollection<Result> expand(PCollection<HBaseQuery> input) {
      checkArgument(serializableConfiguration != null, "withConfiguration() is required");
      return input.apply(ParDo.of(new HBaseReadSplittableDoFn(serializableConfiguration)));
    }

    private SerializableConfiguration serializableConfiguration;
  }

  static class HBaseSource extends BoundedSource<Result> {
    private final Read read;
    @Nullable private Long estimatedSizeBytes;

    HBaseSource(Read read, @Nullable Long estimatedSizeBytes) {
      this.read = read;
      this.estimatedSizeBytes = estimatedSizeBytes;
    }

    HBaseSource withStartKey(ByteKey startKey) throws IOException {
      checkNotNull(startKey, "startKey");
      Read newRead =
          new Read(
              read.serializableConfiguration,
              read.tableId,
              new SerializableScan(
                  new Scan(read.serializableScan.get()).setStartRow(startKey.getBytes())));
      return new HBaseSource(newRead, estimatedSizeBytes);
    }

    HBaseSource withEndKey(ByteKey endKey) throws IOException {
      checkNotNull(endKey, "endKey");
      Read newRead =
          new Read(
              read.serializableConfiguration,
              read.tableId,
              new SerializableScan(
                  new Scan(read.serializableScan.get()).setStopRow(endKey.getBytes())));
      return new HBaseSource(newRead, estimatedSizeBytes);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
      if (estimatedSizeBytes == null) {
        try (Connection connection =
            ConnectionFactory.createConnection(read.serializableConfiguration.get())) {
          estimatedSizeBytes =
              HBaseUtils.estimateSizeBytes(
                  connection,
                  read.tableId,
                  HBaseUtils.getByteKeyRange(read.serializableScan.get()));
        }
        LOG.debug(
            "Estimated size {} bytes for table {} and scan {}",
            estimatedSizeBytes,
            read.tableId,
            read.serializableScan.get());
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
                connection, read.tableId, HBaseUtils.getByteKeyRange(read.serializableScan.get()));
        LOG.debug("Suggested {} source(s) based on size", numSplits);
        LOG.debug("Suggested {} source(s) based on number of regions", regionLocations.size());

        List<ByteKeyRange> ranges =
            HBaseUtils.getRanges(
                regionLocations,
                read.tableId,
                HBaseUtils.getByteKeyRange(read.serializableScan.get()));
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
                        read.serializableConfiguration,
                        read.tableId,
                        new SerializableScan(
                            new Scan(read.serializableScan.get())
                                .setStartRow(range.getStartKey().getBytes())
                                .setStopRow(range.getEndKey().getBytes()))),
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
      Scan scan = source.read.serializableScan.get();
      ByteKeyRange range =
          ByteKeyRange.of(
              ByteKey.copyFrom(scan.getStartRow()), ByteKey.copyFrom(scan.getStopRow()));
      rangeTracker = ByteKeyRangeTracker.of(range);
    }

    @Override
    public boolean start() throws IOException {
      HBaseSource source = getCurrentSource();
      Configuration configuration = source.read.serializableConfiguration.get();
      String tableId = source.read.tableId;
      connection = ConnectionFactory.createConnection(configuration);
      TableName tableName = TableName.valueOf(tableId);
      Table table = connection.getTable(tableName);
      // [BEAM-2319] We have to clone the Scan because the underlying scanner may mutate it.
      Scan scanClone = new Scan(source.read.serializableScan.get());
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
    @Nullable
    public final synchronized HBaseSource splitAtFraction(double fraction) {
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
    return new Write(null /* SerializableConfiguration */, "");
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
      checkArgument(configuration != null, "configuration can not be null");
      return new Write(new SerializableConfiguration(configuration), tableId);
    }

    /** Writes to the specified table. */
    public Write withTableId(String tableId) {
      checkArgument(tableId != null, "tableIdcan not be null");
      return new Write(serializableConfiguration, tableId);
    }

    private Write(SerializableConfiguration serializableConfiguration, String tableId) {
      this.serializableConfiguration = serializableConfiguration;
      this.tableId = tableId;
    }

    @Override
    public PDone expand(PCollection<Mutation> input) {
      checkArgument(serializableConfiguration != null, "withConfiguration() is required");
      checkArgument(tableId != null && !tableId.isEmpty(), "withTableId() is required");
      try (Connection connection =
          ConnectionFactory.createConnection(serializableConfiguration.get())) {
        Admin admin = connection.getAdmin();
        checkArgument(
            admin.tableExists(TableName.valueOf(tableId)), "Table %s does not exist", tableId);
      } catch (IOException e) {
        LOG.warn("Error checking whether table {} exists; proceeding.", tableId, e);
      }
      input.apply(ParDo.of(new HBaseWriterFn(tableId, serializableConfiguration)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("configuration", serializableConfiguration.get().toString()));
      builder.add(DisplayData.item("tableId", tableId));
    }

    public String getTableId() {
      return tableId;
    }

    public Configuration getConfiguration() {
      return serializableConfiguration.get();
    }

    private final String tableId;
    private final SerializableConfiguration serializableConfiguration;

    private class HBaseWriterFn extends DoFn<Mutation, Void> {

      HBaseWriterFn(String tableId, SerializableConfiguration serializableConfiguration) {
        this.tableId = checkNotNull(tableId, "tableId");
        this.serializableConfiguration =
            checkNotNull(serializableConfiguration, "serializableConfiguration");
      }

      @Setup
      public void setup() throws Exception {
        connection = ConnectionFactory.createConnection(serializableConfiguration.get());
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

      private final String tableId;
      private final SerializableConfiguration serializableConfiguration;

      private Connection connection;
      private BufferedMutator mutator;

      private long recordsWritten;
    }
  }
}
