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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bounded source and sink for HBase.
 *
 * <p>For more information, see the online documentation at
 * <a href="https://hbase.apache.org/">HBase</a>.
 *
 * <h3>Reading from HBase</h3>
 *
 * <p>The HBase source returns a set of rows from a single table, returning a
 * {@code PCollection<Result>}.
 *
 * <p>To configure a HBase source, you must supply a table id and a {@link Configuration}
 * to identify the HBase instance. By default, {@link HBaseIO.Read} will read all rows in the
 * table. The row range to be read can optionally be restricted using with a {@link Scan} object
 * or using the {@link HBaseIO.Read#withKeyRange}, and a {@link Filter} using
 * {@link HBaseIO.Read#withFilter}, for example:
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
 * <h3>Writing to HBase</h3>
 *
 * <p>The HBase sink executes a set of row mutations on a single table. It takes as input a
 * {@link PCollection PCollection&lt;Mutation&gt;}, where each {@link Mutation} represents an
 * idempotent transformation on a row.
 *
 * <p>To configure a HBase sink, you must supply a table id and a {@link Configuration}
 * to identify the HBase instance, for example:
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
 * <p>The design of the API for HBaseIO is currently related to the BigtableIO one,
 * it can evolve or be different in some aspects, but the idea is that users can easily migrate
 * from one to the other</p>.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class HBaseIO {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseIO.class);

    /** Disallow construction of utility class. */
    private HBaseIO() {
    }

    /**
     * Creates an uninitialized {@link HBaseIO.Read}. Before use, the {@code Read} must be
     * initialized with a
     * {@link HBaseIO.Read#withConfiguration(Configuration)} that specifies
     * the HBase instance, and a {@link HBaseIO.Read#withTableId tableId} that
     * specifies which table to read. A {@link Filter} may also optionally be specified using
     * {@link HBaseIO.Read#withFilter}.
     */
    @Experimental
    public static Read read() {
        return new Read(null, "", new SerializableScan(new Scan()));
    }

    /**
     * A {@link PTransform} that reads from HBase. See the class-level Javadoc on
     * {@link HBaseIO} for more information.
     *
     * @see HBaseIO
     */
    public static class Read extends PTransform<PBegin, PCollection<Result>> {
        /**
         * Returns a new {@link HBaseIO.Read} that will read from the HBase instance
         * indicated by the given configuration.
         */
        public Read withConfiguration(Configuration configuration) {
            checkNotNull(configuration, "conf");
            return new Read(new SerializableConfiguration(configuration),
                    tableId, serializableScan);
        }

        /**
         * Returns a new {@link HBaseIO.Read} that will read from the specified table.
         *
         * <p>Does not modify this object.
         */
        public Read withTableId(String tableId) {
            checkNotNull(tableId, "tableId");
            return new Read(serializableConfiguration, tableId, serializableScan);
        }

        /**
         * Returns a new {@link HBaseIO.Read} that will filter the rows read from HBase
         * using the given scan.
         *
         * <p>Does not modify this object.
         */
        public Read withScan(Scan scan) {
            checkNotNull(scan, "scan");
            return new Read(serializableConfiguration, tableId, new SerializableScan(scan));
        }

        /**
         * Returns a new {@link HBaseIO.Read} that will filter the rows read from HBase
         * using the given row filter.
         *
         * <p>Does not modify this object.
         */
        public Read withFilter(Filter filter) {
            checkNotNull(filter, "filter");
            return withScan(serializableScan.get().setFilter(filter));
        }

        /**
         * Returns a new {@link HBaseIO.Read} that will read only rows in the specified range.
         *
         * <p>Does not modify this object.
         */
        public Read withKeyRange(ByteKeyRange keyRange) {
            checkNotNull(keyRange, "keyRange");
            byte[] startRow = keyRange.getStartKey().getBytes();
            byte[] stopRow = keyRange.getEndKey().getBytes();
            return withScan(serializableScan.get().setStartRow(startRow).setStopRow(stopRow));
        }

        /**
         * Returns a new {@link HBaseIO.Read} that will read only rows in the specified range.
         *
         * <p>Does not modify this object.
         */
        public Read withKeyRange(byte[] startRow, byte[] stopRow) {
            checkNotNull(startRow, "startRow");
            checkNotNull(stopRow, "stopRow");
            ByteKeyRange keyRange =
                    ByteKeyRange.of(ByteKey.copyFrom(startRow), ByteKey.copyFrom(stopRow));
            return withKeyRange(keyRange);
        }

        private Read(SerializableConfiguration serializableConfiguration, String tableId,
                     SerializableScan serializableScan) {
            this.serializableConfiguration = serializableConfiguration;
            this.tableId = tableId;
            this.serializableScan = serializableScan;
        }

        @Override
        public PCollection<Result> expand(PBegin input) {
            HBaseSource source = new HBaseSource(this, null /* estimatedSizeBytes */);
            return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
        }

        @Override
        public void validate(PipelineOptions options) {
            checkArgument(serializableConfiguration != null,
                    "Configuration not provided");
            checkArgument(!tableId.isEmpty(), "Table ID not specified");
            try (Connection connection = ConnectionFactory.createConnection(
                    serializableConfiguration.get())) {
                Admin admin = connection.getAdmin();
                checkArgument(admin.tableExists(TableName.valueOf(tableId)),
                        "Table %s does not exist", tableId);
            } catch (IOException e) {
                LOG.warn("Error checking whether table {} exists; proceeding.", tableId, e);
            }
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("configuration",
                    serializableConfiguration.get().toString()));
            builder.add(DisplayData.item("tableId", tableId));
            builder.addIfNotNull(DisplayData.item("scan", serializableScan.get().toString()));
        }

        public String getTableId() {
            return tableId;
        }

        public Configuration getConfiguration() {
            return serializableConfiguration.get();
        }

        /**
         * Returns the range of keys that will be read from the table.
         */
        public ByteKeyRange getKeyRange() {
            byte[] startRow = serializableScan.get().getStartRow();
            byte[] stopRow = serializableScan.get().getStopRow();
            return ByteKeyRange.of(ByteKey.copyFrom(startRow), ByteKey.copyFrom(stopRow));
        }

        private final SerializableConfiguration serializableConfiguration;
        private final String tableId;
        private final SerializableScan serializableScan;
    }

    static class HBaseSource extends BoundedSource<Result> {
        private final Read read;
        @Nullable private Long estimatedSizeBytes;

        HBaseSource(Read read, @Nullable Long estimatedSizeBytes) {
            this.read = read;
            this.estimatedSizeBytes = estimatedSizeBytes;
        }

        @Override
        public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
            if (estimatedSizeBytes == null) {
                estimatedSizeBytes = estimateSizeBytes();
                LOG.debug("Estimated size {} bytes for table {} and scan {}", estimatedSizeBytes,
                        read.tableId, read.serializableScan.get());
            }
            return estimatedSizeBytes;
        }

        /**
         * This estimates the real size, it can be the compressed size depending on the HBase
         * configuration.
         */
        private long estimateSizeBytes() throws Exception {
            // This code is based on RegionSizeCalculator in hbase-server
            long estimatedSizeBytes = 0L;
            Configuration configuration = this.read.serializableConfiguration.get();
            try (Connection connection = ConnectionFactory.createConnection(configuration)) {
                // filter regions for the given table/scan
                List<HRegionLocation> regionLocations = getRegionLocations(connection);

                // builds set of regions who are part of the table scan
                Set<byte[]> tableRegions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
                for (HRegionLocation regionLocation : regionLocations) {
                    tableRegions.add(regionLocation.getRegionInfo().getRegionName());
                }

                // calculate estimated size for the regions
                Admin admin = connection.getAdmin();
                ClusterStatus clusterStatus = admin.getClusterStatus();
                Collection<ServerName> servers = clusterStatus.getServers();
                for (ServerName serverName : servers) {
                    ServerLoad serverLoad = clusterStatus.getLoad(serverName);
                    for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
                        byte[] regionId = regionLoad.getName();
                        if (tableRegions.contains(regionId)) {
                            long regionSizeBytes = regionLoad.getStorefileSizeMB() * 1_048_576L;
                            estimatedSizeBytes += regionSizeBytes;
                        }
                    }
                }
            }
            return estimatedSizeBytes;
        }

        private List<HRegionLocation> getRegionLocations(Connection connection) throws Exception {
            final Scan scan = read.serializableScan.get();
            byte[] startRow = scan.getStartRow();
            byte[] stopRow = scan.getStopRow();

            final List<HRegionLocation> regionLocations = new ArrayList<>();

            final boolean scanWithNoLowerBound = startRow.length == 0;
            final boolean scanWithNoUpperBound = stopRow.length == 0;

            TableName tableName = TableName.valueOf(read.tableId);
            RegionLocator regionLocator = connection.getRegionLocator(tableName);
            List<HRegionLocation> tableRegionInfos = regionLocator.getAllRegionLocations();
            for (HRegionLocation regionLocation : tableRegionInfos) {
                final byte[] startKey = regionLocation.getRegionInfo().getStartKey();
                final byte[] endKey = regionLocation.getRegionInfo().getEndKey();
                boolean isLastRegion = endKey.length == 0;
                // filters regions who are part of the scan
                if ((scanWithNoLowerBound
                        || isLastRegion || Bytes.compareTo(startRow, endKey) < 0)
                        && (scanWithNoUpperBound || Bytes.compareTo(stopRow, startKey) > 0)) {
                    regionLocations.add(regionLocation);
                }
            }

            return regionLocations;
        }

        private List<HBaseSource>
            splitBasedOnRegions(List<HRegionLocation> regionLocations, int numSplits)
                throws Exception {
            final Scan scan = read.serializableScan.get();
            byte[] startRow = scan.getStartRow();
            byte[] stopRow = scan.getStopRow();

            final List<HBaseSource> sources = new ArrayList<>(numSplits);
            final boolean scanWithNoLowerBound = startRow.length == 0;
            final boolean scanWithNoUpperBound = stopRow.length == 0;

            for (HRegionLocation regionLocation : regionLocations) {
                final byte[] startKey = regionLocation.getRegionInfo().getStartKey();
                final byte[] endKey = regionLocation.getRegionInfo().getEndKey();
                boolean isLastRegion = endKey.length == 0;
                String host = regionLocation.getHostnamePort();

                final byte[] splitStart = (scanWithNoLowerBound
                        || Bytes.compareTo(startKey, startRow) >= 0) ? startKey : startRow;
                final byte[] splitStop =
                        (scanWithNoUpperBound || Bytes.compareTo(endKey, stopRow) <= 0)
                                && !isLastRegion ? endKey : stopRow;

                LOG.debug("{} {} {} {} {}", sources.size(), host, read.tableId,
                        Bytes.toString(splitStart), Bytes.toString(splitStop));

                // We need to create a new copy of the scan and read to add the new ranges
                Scan newScan = new Scan(scan).setStartRow(splitStart).setStopRow(splitStop);
                Read newRead = new Read(read.serializableConfiguration, read.tableId,
                        new SerializableScan(newScan));
                sources.add(new HBaseSource(newRead, estimatedSizeBytes));
            }
            return sources;
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

            try (Connection connection = ConnectionFactory.createConnection(
                    read.getConfiguration())) {
                List<HRegionLocation> regionLocations = getRegionLocations(connection);
                int realNumSplits =
                        numSplits < regionLocations.size() ? regionLocations.size() : numSplits;
                LOG.debug("Suggested {} bundle(s) based on size", numSplits);
                LOG.debug("Suggested {} bundle(s) based on number of regions",
                        regionLocations.size());
                final List<HBaseSource> sources = splitBasedOnRegions(regionLocations,
                        realNumSplits);
                LOG.debug("Split into {} bundle(s)", sources.size());
                if (numSplits >= 1) {
                    return sources;
                }
                return Collections.singletonList(this);
            }
        }

        @Override
        public BoundedReader<Result> createReader(PipelineOptions pipelineOptions)
                throws IOException {
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
        public Coder<Result> getDefaultOutputCoder() {
            return HBaseResultCoder.of();
        }
    }

    private static class HBaseReader extends BoundedSource.BoundedReader<Result> {
        private final HBaseSource source;
        private Connection connection;
        private ResultScanner scanner;
        private Iterator<Result> iter;
        private Result current;
        private long recordsReturned;

        HBaseReader(HBaseSource source) {
            this.source = source;
        }

        @Override
        public boolean start() throws IOException {
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
        public boolean advance() throws IOException {
            boolean hasRecord = iter.hasNext();
            if (hasRecord) {
                current = iter.next();
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
        public BoundedSource<Result> getCurrentSource() {
            return source;
        }
    }

    /**
     * Creates an uninitialized {@link HBaseIO.Write}. Before use, the {@code Write} must be
     * initialized with a
     * {@link HBaseIO.Write#withConfiguration(Configuration)} that specifies
     * the destination HBase instance, and a {@link HBaseIO.Write#withTableId tableId}
     * that specifies which table to write.
     */
    public static Write write() {
        return new Write(null /* SerializableConfiguration */, "");
    }

    /**
     * A {@link PTransform} that writes to HBase. See the class-level Javadoc on
     * {@link HBaseIO} for more information.
     *
     * @see HBaseIO
     */
    public static class Write extends PTransform<PCollection<Mutation>, PDone> {
        /**
         * Returns a new {@link HBaseIO.Write} that will write to the HBase instance
         * indicated by the given Configuration, and using any other specified customizations.
         *
         * <p>Does not modify this object.
         */
        public Write withConfiguration(Configuration configuration) {
            checkNotNull(configuration, "conf");
            return new Write(new SerializableConfiguration(configuration), tableId);
        }

        /**
         * Returns a new {@link HBaseIO.Write} that will write to the specified table.
         *
         * <p>Does not modify this object.
         */
        public Write withTableId(String tableId) {
            checkNotNull(tableId, "tableId");
            return new Write(serializableConfiguration, tableId);
        }

        private Write(SerializableConfiguration serializableConfiguration, String tableId) {
            this.serializableConfiguration = serializableConfiguration;
            this.tableId = tableId;
        }

        @Override
        public PDone expand(PCollection<Mutation> input) {
            input.apply(ParDo.of(new HBaseWriterFn(tableId, serializableConfiguration)));
            return PDone.in(input.getPipeline());
        }

        @Override
        public void validate(PipelineOptions options) {
            checkArgument(serializableConfiguration != null, "Configuration not specified");
            checkArgument(!tableId.isEmpty(), "Table ID not specified");
            try (Connection connection = ConnectionFactory.createConnection(
                    serializableConfiguration.get())) {
                Admin admin = connection.getAdmin();
                checkArgument(admin.tableExists(TableName.valueOf(tableId)),
                        "Table %s does not exist", tableId);
            } catch (IOException e) {
                LOG.warn("Error checking whether table {} exists; proceeding.", tableId, e);
            }
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("configuration",
                    serializableConfiguration.get().toString()));
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

            public HBaseWriterFn(String tableId,
                                 SerializableConfiguration serializableConfiguration) {
                this.tableId = checkNotNull(tableId, "tableId");
                this.serializableConfiguration = checkNotNull(serializableConfiguration,
                        "serializableConfiguration");
            }

            @Setup
            public void setup() throws Exception {
                connection = ConnectionFactory.createConnection(serializableConfiguration.get());
            }

            @StartBundle
            public void startBundle(StartBundleContext c) throws IOException {
                BufferedMutatorParams params =
                    new BufferedMutatorParams(TableName.valueOf(tableId));
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
