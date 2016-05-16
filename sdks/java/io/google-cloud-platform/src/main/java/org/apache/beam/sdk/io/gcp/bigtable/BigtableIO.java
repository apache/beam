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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Sink.WriteOperation;
import org.apache.beam.sdk.io.Sink.Writer;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.io.range.ByteKeyRangeTracker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.Nullable;

/**
 * A bounded source and sink for Google Cloud Bigtable.
 *
 * <p>For more information, see the online documentation at
 * <a href="https://cloud.google.com/bigtable/">Google Cloud Bigtable</a>.
 *
 * <h3>Reading from Cloud Bigtable</h3>
 *
 * <p>The Bigtable source returns a set of rows from a single table, returning a
 * {@code PCollection<Row>}.
 *
 * <p>To configure a Cloud Bigtable source, you must supply a table id and a {@link BigtableOptions}
 * or builder configured with the project and other information necessary to identify the
 * Bigtable cluster. A {@link RowFilter} may also optionally be specified using
 * {@link BigtableIO.Read#withRowFilter}. For example:
 *
 * <pre>{@code
 * BigtableOptions.Builder optionsBuilder =
 *     new BigtableOptions.Builder()
 *         .setProjectId("project")
 *         .setClusterId("cluster")
 *         .setZoneId("zone");
 *
 * Pipeline p = ...;
 *
 * // Scan the entire table.
 * p.apply("read",
 *     BigtableIO.read()
 *         .withBigtableOptions(optionsBuilder)
 *         .withTableId("table"));
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
 * Bigtable cluster, for example:
 *
 * <pre>{@code
 * BigtableOptions.Builder optionsBuilder =
 *     new BigtableOptions.Builder()
 *         .setProjectId("project")
 *         .setClusterId("cluster")
 *         .setZoneId("zone");
 *
 * PCollection<KV<ByteString, Iterable<Mutation>>> data = ...;
 *
 * data.apply("write",
 *     BigtableIO.write()
 *         .withBigtableOptions(optionsBuilder)
 *         .withTableId("table"));
 * }</pre>
 *
 * <h3>Experimental</h3>
 *
 * <p>This connector for Cloud Bigtable is considered experimental and may break or receive
 * backwards-incompatible changes in future versions of the Cloud Dataflow SDK. Cloud Bigtable is
 * in Beta, and thus it may introduce breaking changes in future revisions of its service or APIs.
 *
 * <h3>Permissions</h3>
 *
 * <p>Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * Dataflow job. Please refer to the documentation of corresponding
 * {@link PipelineRunner PipelineRunners} for more details.
 */
@Experimental
public class BigtableIO {
  private static final Logger logger = LoggerFactory.getLogger(BigtableIO.class);

  /**
   * Creates an uninitialized {@link BigtableIO.Read}. Before use, the {@code Read} must be
   * initialized with a
   * {@link BigtableIO.Read#withBigtableOptions(BigtableOptions) BigtableOptions} that specifies
   * the source Cloud Bigtable cluster, and a {@link BigtableIO.Read#withTableId tableId} that
   * specifies which table to read. A {@link RowFilter} may also optionally be specified using
   * {@link BigtableIO.Read#withRowFilter}.
   */
  @Experimental
  public static Read read() {
    return new Read(null, "", null, null);
  }

  /**
   * Creates an uninitialized {@link BigtableIO.Write}. Before use, the {@code Write} must be
   * initialized with a
   * {@link BigtableIO.Write#withBigtableOptions(BigtableOptions) BigtableOptions} that specifies
   * the destination Cloud Bigtable cluster, and a {@link BigtableIO.Write#withTableId tableId} that
   * specifies which table to write.
   */
  @Experimental
  public static Write write() {
    return new Write(null, "", null);
  }

  /**
   * A {@link PTransform} that reads from Google Cloud Bigtable. See the class-level Javadoc on
   * {@link BigtableIO} for more information.
   *
   * @see BigtableIO
   */
  @Experimental
  public static class Read extends PTransform<PBegin, PCollection<Row>> {
    /**
     * Returns a new {@link BigtableIO.Read} that will read from the Cloud Bigtable cluster
     * indicated by the given options, and using any other specified customizations.
     *
     * <p>Does not modify this object.
     */
    public Read withBigtableOptions(BigtableOptions options) {
      checkNotNull(options, "options");
      return withBigtableOptions(options.toBuilder());
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the Cloud Bigtable cluster
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
      BigtableOptions.Builder clonedBuilder = optionsBuilder.build().toBuilder();
      BigtableOptions optionsWithAgent = clonedBuilder.setUserAgent(getUserAgent()).build();
      return new Read(optionsWithAgent, tableId, filter, bigtableService);
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will filter the rows read from Cloud Bigtable
     * using the given row filter.
     *
     * <p>Does not modify this object.
     */
    public Read withRowFilter(RowFilter filter) {
      checkNotNull(filter, "filter");
      return new Read(options, tableId, filter, bigtableService);
    }

    /**
     * Returns a new {@link BigtableIO.Read} that will read from the specified table.
     *
     * <p>Does not modify this object.
     */
    public Read withTableId(String tableId) {
      checkNotNull(tableId, "tableId");
      return new Read(options, tableId, filter, bigtableService);
    }

    /**
     * Returns the Google Cloud Bigtable cluster being read from, and other parameters.
     */
    public BigtableOptions getBigtableOptions() {
      return options;
    }

    /**
     * Returns the table being read from.
     */
    public String getTableId() {
      return tableId;
    }

    @Override
    public PCollection<Row> apply(PBegin input) {
      BigtableSource source =
          new BigtableSource(getBigtableService(), tableId, filter, ByteKeyRange.ALL_KEYS, null);
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
    }

    @Override
    public void validate(PBegin input) {
      checkArgument(options != null, "BigtableOptions not specified");
      checkArgument(!tableId.isEmpty(), "Table ID not specified");
      try {
        checkArgument(
            getBigtableService().tableExists(tableId), "Table %s does not exist", tableId);
      } catch (IOException e) {
        logger.warn("Error checking whether table {} exists; proceeding.", tableId, e);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.add(DisplayData.item("tableId", tableId)
        .withLinkUrl("Table ID"));

      if (options != null) {
        builder.add(DisplayData.item("bigtableOptions", options.toString())
          .withLabel("Bigtable Options"));
      }

      if (filter != null) {
        builder.add(DisplayData.item("rowFilter", filter.toString())
          .withLabel("Table Row Filter"));
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Read.class)
          .add("options", options)
          .add("tableId", tableId)
          .add("filter", filter)
          .toString();
    }

    /////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Used to define the Cloud Bigtable cluster and any options for the networking layer.
     * Cannot actually be {@code null} at validation time, but may start out {@code null} while
     * source is being built.
     */
    @Nullable private final BigtableOptions options;
    private final String tableId;
    @Nullable private final RowFilter filter;
    @Nullable private final BigtableService bigtableService;

    private Read(
        @Nullable BigtableOptions options,
        String tableId,
        @Nullable RowFilter filter,
        @Nullable BigtableService bigtableService) {
      this.options = options;
      this.tableId = checkNotNull(tableId, "tableId");
      this.filter = filter;
      this.bigtableService = bigtableService;
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
      return new Read(options, tableId, filter, bigtableService);
    }

    /**
     * Helper function that either returns the mock Bigtable service supplied by
     * {@link #withBigtableService} or creates and returns an implementation that talks to
     * {@code Cloud Bigtable}.
     */
    private BigtableService getBigtableService() {
      if (bigtableService != null) {
        return bigtableService;
      }
      return new BigtableServiceImpl(options);
    }
  }

  /**
   * A {@link PTransform} that writes to Google Cloud Bigtable. See the class-level Javadoc on
   * {@link BigtableIO} for more information.
   *
   * @see BigtableIO
   */
  @Experimental
  public static class Write
      extends PTransform<PCollection<KV<ByteString, Iterable<Mutation>>>, PDone> {
    /**
     * Used to define the Cloud Bigtable cluster and any options for the networking layer.
     * Cannot actually be {@code null} at validation time, but may start out {@code null} while
     * source is being built.
     */
    @Nullable private final BigtableOptions options;
    private final String tableId;
    @Nullable private final BigtableService bigtableService;

    private Write(
        @Nullable BigtableOptions options,
        String tableId,
        @Nullable BigtableService bigtableService) {
      this.options = options;
      this.tableId = checkNotNull(tableId, "tableId");
      this.bigtableService = bigtableService;
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write to the Cloud Bigtable cluster
     * indicated by the given options, and using any other specified customizations.
     *
     * <p>Does not modify this object.
     */
    public Write withBigtableOptions(BigtableOptions options) {
      checkNotNull(options, "options");
      return withBigtableOptions(options.toBuilder());
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write to the Cloud Bigtable cluster
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
      BigtableOptions.Builder clonedBuilder = optionsBuilder.build().toBuilder();
      BigtableOptions optionsWithAgent = clonedBuilder.setUserAgent(getUserAgent()).build();
      return new Write(optionsWithAgent, tableId, bigtableService);
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write to the specified table.
     *
     * <p>Does not modify this object.
     */
    public Write withTableId(String tableId) {
      checkNotNull(tableId, "tableId");
      return new Write(options, tableId, bigtableService);
    }

    /**
     * Returns the Google Cloud Bigtable cluster being written to, and other parameters.
     */
    public BigtableOptions getBigtableOptions() {
      return options;
    }

    /**
     * Returns the table being written to.
     */
    public String getTableId() {
      return tableId;
    }

    @Override
    public PDone apply(PCollection<KV<ByteString, Iterable<Mutation>>> input) {
      Sink sink = new Sink(tableId, getBigtableService());
      return input.apply(org.apache.beam.sdk.io.Write.to(sink));
    }

    @Override
    public void validate(PCollection<KV<ByteString, Iterable<Mutation>>> input) {
      checkArgument(options != null, "BigtableOptions not specified");
      checkArgument(!tableId.isEmpty(), "Table ID not specified");
      try {
        checkArgument(
            getBigtableService().tableExists(tableId), "Table %s does not exist", tableId);
      } catch (IOException e) {
        logger.warn("Error checking whether table {} exists; proceeding.", tableId, e);
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
      return new Write(options, tableId, bigtableService);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.add(DisplayData.item("tableId", tableId)
        .withLabel("Table ID"));

      if (options != null) {
        builder.add(DisplayData.item("bigtableOptions", options.toString())
          .withLabel("Bigtable Options"));
      }
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Write.class)
          .add("options", options)
          .add("tableId", tableId)
          .toString();
    }

    /**
     * Helper function that either returns the mock Bigtable service supplied by
     * {@link #withBigtableService} or creates and returns an implementation that talks to
     * {@code Cloud Bigtable}.
     */
    private BigtableService getBigtableService() {
      if (bigtableService != null) {
        return bigtableService;
      }
      return new BigtableServiceImpl(options);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////
  /** Disallow construction of utility class. */
  private BigtableIO() {}

  static class BigtableSource extends BoundedSource<Row> {
    public BigtableSource(
        BigtableService service,
        String tableId,
        @Nullable RowFilter filter,
        ByteKeyRange range,
        Long estimatedSizeBytes) {
      this.service = service;
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
    private final BigtableService service;
    @Nullable private final String tableId;
    @Nullable private final RowFilter filter;
    private final ByteKeyRange range;
    @Nullable private Long estimatedSizeBytes;
    @Nullable private transient List<SampleRowKeysResponse> sampleRowKeys;

    protected BigtableSource withStartKey(ByteKey startKey) {
      checkNotNull(startKey, "startKey");
      return new BigtableSource(
          service, tableId, filter, range.withStartKey(startKey), estimatedSizeBytes);
    }

    protected BigtableSource withEndKey(ByteKey endKey) {
      checkNotNull(endKey, "endKey");
      return new BigtableSource(
          service, tableId, filter, range.withEndKey(endKey), estimatedSizeBytes);
    }

    protected BigtableSource withEstimatedSizeBytes(Long estimatedSizeBytes) {
      checkNotNull(estimatedSizeBytes, "estimatedSizeBytes");
      return new BigtableSource(service, tableId, filter, range, estimatedSizeBytes);
    }

    /**
     * Makes an API call to the Cloud Bigtable service that gives information about tablet key
     * boundaries and estimated sizes. We can use these samples to ensure that splits are on
     * different tablets, and possibly generate sub-splits within tablets.
     */
    private List<SampleRowKeysResponse> getSampleRowKeys() throws IOException {
      return service.getSampleRowKeys(this);
    }

    @Override
    public List<BigtableSource> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      // Update the desiredBundleSizeBytes in order to limit the
      // number of splits to maximumNumberOfSplits.
      long maximumNumberOfSplits = 4000;
      long sizeEstimate = getEstimatedSizeBytes(options);
      desiredBundleSizeBytes =
          Math.max(sizeEstimate / maximumNumberOfSplits, desiredBundleSizeBytes);

      // Delegate to testable helper.
      return splitIntoBundlesBasedOnSamples(desiredBundleSizeBytes, getSampleRowKeys());
    }

    /** Helper that splits this source into bundles based on Cloud Bigtable sampled row keys. */
    private List<BigtableSource> splitIntoBundlesBasedOnSamples(
        long desiredBundleSizeBytes, List<SampleRowKeysResponse> sampleRowKeys) {
      // There are no regions, or no samples available. Just scan the entire range.
      if (sampleRowKeys.isEmpty()) {
        logger.info("Not splitting source {} because no sample row keys are available.", this);
        return Collections.singletonList(this);
      }

      logger.info(
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
        ByteKey responseEndKey = ByteKey.of(response.getRowKey());
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
      logger.info("Generated {} splits. First split: {}", ret.size(), ret.get(0));
      return ret;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      // Delegate to testable helper.
      if (estimatedSizeBytes == null) {
        estimatedSizeBytes = getEstimatedSizeBytesBasedOnSamples(getSampleRowKeys());
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
        ByteKey currentEndKey = ByteKey.of(response.getRowKey());
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

    /**
     * Cloud Bigtable returns query results ordered by key.
     */
    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return true;
    }

    @Override
    public BoundedReader<Row> createReader(PipelineOptions options) throws IOException {
      return new BigtableReader(this, service);
    }

    @Override
    public void validate() {
      checkArgument(!tableId.isEmpty(), "tableId cannot be empty");
    }

    @Override
    public Coder<Row> getDefaultOutputCoder() {
      return ProtoCoder.of(Row.class);
    }

    /** Helper that splits the specified range in this source into bundles. */
    private List<BigtableSource> splitKeyRangeIntoBundleSizedSubranges(
        long sampleSizeBytes, long desiredBundleSizeBytes, ByteKeyRange range) {
      // Catch the trivial cases. Split is small enough already, or this is the last region.
      logger.debug(
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
              && rangeTracker.tryReturnRecordAt(true, ByteKey.of(reader.getCurrentRow().getKey()));
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
              && rangeTracker.tryReturnRecordAt(true, ByteKey.of(reader.getCurrentRow().getKey()));
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
      logger.info("Closing reader after reading {} records.", recordsReturned);
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
    public final synchronized BigtableSource splitAtFraction(double fraction) {
      ByteKey splitKey;
      try {
        splitKey = source.getRange().interpolateKey(fraction);
      } catch (IllegalArgumentException e) {
        logger.info("%s: Failed to interpolate key for fraction %s.", source.getRange(), fraction);
        return null;
      }
      logger.debug(
          "Proposing to split {} at fraction {} (key {})", rangeTracker, fraction, splitKey);
      if (!rangeTracker.trySplitAtPosition(splitKey)) {
        return null;
      }
      BigtableSource primary = source.withEndKey(splitKey);
      BigtableSource residual = source.withStartKey(splitKey);
      this.source = primary;
      return residual;
    }
  }

  private static class Sink
      extends org.apache.beam.sdk.io.Sink<KV<ByteString, Iterable<Mutation>>> {

    public Sink(String tableId, BigtableService bigtableService) {
      this.tableId = checkNotNull(tableId, "tableId");
      this.bigtableService = checkNotNull(bigtableService, "bigtableService");
    }

    public String getTableId() {
      return tableId;
    }

    public BigtableService getBigtableService() {
      return bigtableService;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Sink.class)
          .add("bigtableService", bigtableService)
          .add("tableId", tableId)
          .toString();
    }

    ///////////////////////////////////////////////////////////////////////////////
    private final String tableId;
    private final BigtableService bigtableService;

    @Override
    public WriteOperation<KV<ByteString, Iterable<Mutation>>, Long> createWriteOperation(
        PipelineOptions options) {
      return new BigtableWriteOperation(this);
    }

    /** Does nothing, as it is redundant with {@link Write#validate}. */
    @Override
    public void validate(PipelineOptions options) {}
  }

  private static class BigtableWriteOperation
      extends WriteOperation<KV<ByteString, Iterable<Mutation>>, Long> {
    private final Sink sink;

    public BigtableWriteOperation(Sink sink) {
      this.sink = sink;
    }

    @Override
    public Writer<KV<ByteString, Iterable<Mutation>>, Long> createWriter(PipelineOptions options)
        throws Exception {
      return new BigtableWriter(this);
    }

    @Override
    public void initialize(PipelineOptions options) {}

    @Override
    public void finalize(Iterable<Long> writerResults, PipelineOptions options) {
      long count = 0;
      for (Long value : writerResults) {
        value += count;
      }
      logger.debug("Wrote {} elements to BigtableIO.Sink {}", sink);
    }

    @Override
    public Sink getSink() {
      return sink;
    }

    @Override
    public Coder<Long> getWriterResultCoder() {
      return VarLongCoder.of();
    }
  }

  private static class BigtableWriter extends Writer<KV<ByteString, Iterable<Mutation>>, Long> {
    private final BigtableWriteOperation writeOperation;
    private final Sink sink;
    private BigtableService.Writer bigtableWriter;
    private long recordsWritten;
    private final ConcurrentLinkedQueue<BigtableWriteException> failures;

    public BigtableWriter(BigtableWriteOperation writeOperation) {
      this.writeOperation = writeOperation;
      this.sink = writeOperation.getSink();
      this.failures = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void open(String uId) throws Exception {
      bigtableWriter = sink.getBigtableService().openForWriting(sink.getTableId());
      recordsWritten = 0;
    }

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
      logger.error(message);
      throw new IOException(message);
    }

    @Override
    public void write(KV<ByteString, Iterable<Mutation>> rowMutations) throws Exception {
      checkForFailures();
      Futures.addCallback(
          bigtableWriter.writeRecord(rowMutations), new WriteExceptionCallback(rowMutations));
      ++recordsWritten;
    }

    @Override
    public Long close() throws Exception {
      bigtableWriter.close();
      bigtableWriter = null;
      checkForFailures();
      logger.info("Wrote {} records", recordsWritten);
      return recordsWritten;
    }

    @Override
    public WriteOperation<KV<ByteString, Iterable<Mutation>>, Long> getWriteOperation() {
      return writeOperation;
    }

    private class WriteExceptionCallback implements FutureCallback<Empty> {
      private final KV<ByteString, Iterable<Mutation>> value;

      public WriteExceptionCallback(KV<ByteString, Iterable<Mutation>> value) {
        this.value = value;
      }

      @Override
      public void onFailure(Throwable cause) {
        failures.add(new BigtableWriteException(value, cause));
      }

      @Override
      public void onSuccess(Empty produced) {}
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
   * A helper function to produce a Cloud Bigtable user agent string.
   */
  private static String getUserAgent() {
    String javaVersion = System.getProperty("java.specification.version");
    ReleaseInfo info = ReleaseInfo.getReleaseInfo();
    return String.format(
        "%s/%s (%s); %s",
        info.getName(),
        info.getVersion(),
        javaVersion,
        "0.2.3" /* TODO get Bigtable client version directly from jar. */);
  }
}
