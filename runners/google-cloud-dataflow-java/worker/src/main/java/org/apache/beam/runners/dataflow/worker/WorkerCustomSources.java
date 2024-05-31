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
package org.apache.beam.runners.dataflow.worker;

import static com.google.api.client.util.Base64.decodeBase64;
import static com.google.api.client.util.Base64.encodeBase64String;
import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.apache.beam.runners.dataflow.util.Structs.getString;
import static org.apache.beam.runners.dataflow.util.Structs.getStrings;
import static org.apache.beam.sdk.util.SerializableUtils.deserializeFromByteArray;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.util.Base64;
import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import com.google.api.services.dataflow.model.ApproximateSplitRequest;
import com.google.api.services.dataflow.model.DerivedSource;
import com.google.api.services.dataflow.model.DynamicSourceSplit;
import com.google.api.services.dataflow.model.ReportedParallelism;
import com.google.api.services.dataflow.model.SourceMetadata;
import com.google.api.services.dataflow.model.SourceOperationResponse;
import com.google.api.services.dataflow.model.SourceSplitOptions;
import com.google.api.services.dataflow.model.SourceSplitRequest;
import com.google.api.services.dataflow.model.SourceSplitResponse;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.internal.CustomSources;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class for supporting sources defined as {@code Source}.
 *
 * <p>Provides a bridge between the high-level {@code Source} API and the low-level {@code
 * CloudSource} class.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WorkerCustomSources {
  private static final String SERIALIZED_SOURCE = "serialized_source";
  @VisibleForTesting static final String SERIALIZED_SOURCE_SPLITS = "serialized_source_splits";
  private static final long DEFAULT_DESIRED_BUNDLE_SIZE_BYTES = 64 * (1 << 20);

  // The maximum number of bundles we are willing to return to the service in one response.
  static final int DEFAULT_NUM_BUNDLES_LIMIT = 100;

  /**
   * The current limit on the size of a ReportWorkItemStatus RPC to Google Cloud Dataflow, which
   * includes the initial splits, is 20 MB.
   */
  public static final long DATAFLOW_SPLIT_RESPONSE_API_SIZE_LIMIT = 20 * (1 << 20);

  private static final Logger LOG = LoggerFactory.getLogger(WorkerCustomSources.class);

  /**
   * A {@code DynamicSplitResult} specified explicitly by a pair of {@code BoundedSource} objects
   * describing the primary and residual sources.
   */
  public static final class BoundedSourceSplit<T> implements NativeReader.DynamicSplitResult {
    public final BoundedSource<T> primary;
    public final BoundedSource<T> residual;

    public BoundedSourceSplit(BoundedSource<T> primary, BoundedSource<T> residual) {
      this.primary = primary;
      this.residual = residual;
    }

    @Override
    public String toString() {
      return String.format("<primary: %s; residual: %s>", primary, residual);
    }
  }

  public static DynamicSourceSplit toSourceSplit(BoundedSourceSplit<?> sourceSplitResult) {
    DynamicSourceSplit sourceSplit = new DynamicSourceSplit();
    com.google.api.services.dataflow.model.Source primarySource;
    com.google.api.services.dataflow.model.Source residualSource;
    try {
      primarySource = serializeSplitToCloudSource(sourceSplitResult.primary);
      residualSource = serializeSplitToCloudSource(sourceSplitResult.residual);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize one of the parts of the source split", e);
    }
    sourceSplit.setPrimary(
        new DerivedSource()
            .setDerivationMode("SOURCE_DERIVATION_MODE_INDEPENDENT")
            .setSource(primarySource));
    sourceSplit.setResidual(
        new DerivedSource()
            .setDerivationMode("SOURCE_DERIVATION_MODE_INDEPENDENT")
            .setSource(residualSource));
    return sourceSplit;
  }

  /**
   * Version of {@link CustomSources#serializeToCloudSource(Source, PipelineOptions)} intended for
   * use on splits of {@link BoundedSource}.
   */
  private static com.google.api.services.dataflow.model.Source serializeSplitToCloudSource(
      BoundedSource<?> source) throws Exception {
    com.google.api.services.dataflow.model.Source cloudSource =
        new com.google.api.services.dataflow.model.Source();
    cloudSource.setSpec(CloudObject.forClass(CustomSources.class));
    addString(
        cloudSource.getSpec(), SERIALIZED_SOURCE, encodeBase64String(serializeToByteArray(source)));
    SourceMetadata metadata = new SourceMetadata();
    // Size estimation is best effort so we continue even if it fails here.
    try {
      long estimatedSize = source.getEstimatedSizeBytes(PipelineOptionsFactory.create());
      if (estimatedSize >= 0) {
        metadata.setEstimatedSizeBytes(estimatedSize);
      } else {
        LOG.warn(
            "Ignoring negative estimated size {} produced by source {}", estimatedSize, source);
      }
    } catch (Exception e) {
      LOG.warn("Size estimation of the source failed: " + source, e);
    }
    cloudSource.setMetadata(metadata);
    return cloudSource;
  }

  /**
   * Executes a protocol-level split {@code SourceOperationRequest} for bounded sources by
   * deserializing its source to a {@code BoundedSource}, splitting it, and serializing results
   * back.
   *
   * <p>When the splits produced by this function are too large to be serialized to the Dataflow
   * API, splitting is retried once with an increase in the desired bundle size. This change aims to
   * work around API limitations on split size.
   */
  public static SourceOperationResponse performSplit(
      SourceSplitRequest request, PipelineOptions options) throws Exception {
    return performSplitWithApiLimit(
        request, options, DEFAULT_NUM_BUNDLES_LIMIT, DATAFLOW_SPLIT_RESPONSE_API_SIZE_LIMIT);
  }

  /**
   * A helper method like {@link #performSplit(SourceSplitRequest, PipelineOptions)} but that allows
   * overriding the API size limit for testing.
   */
  static SourceOperationResponse performSplitWithApiLimit(
      SourceSplitRequest request, PipelineOptions options, int numBundlesLimit, long apiByteLimit)
      throws Exception {
    // Compute the desired bundle size given by the service, or default if none was provided.
    long desiredBundleSizeBytes = DEFAULT_DESIRED_BUNDLE_SIZE_BYTES;
    SourceSplitOptions splitOptions = request.getOptions();
    if (splitOptions != null && splitOptions.getDesiredBundleSizeBytes() != null) {
      desiredBundleSizeBytes = splitOptions.getDesiredBundleSizeBytes();
    }

    Source<?> anySource = deserializeFromCloudSource(request.getSource().getSpec());
    checkArgument(
        anySource instanceof BoundedSource, "Cannot split a non-Bounded source: %s", anySource);
    return performSplitTyped(
        options,
        (BoundedSource<?>) anySource,
        desiredBundleSizeBytes,
        numBundlesLimit,
        apiByteLimit);
  }

  private static <T> SourceOperationResponse performSplitTyped(
      PipelineOptions options,
      BoundedSource<T> source,
      long desiredBundleSizeBytes,
      int numBundlesLimit,
      long apiByteLimit)
      throws Exception {
    // Try to split normally
    List<BoundedSource<T>> bundles = splitAndValidate(source, desiredBundleSizeBytes, options);

    // If serialized size is too big, try splitting with a proportionally larger desiredBundleSize
    // to reduce the oversplitting.
    long serializedSize =
        DataflowApiUtils.computeSerializedSizeBytes(wrapIntoSourceSplitResponse(bundles));

    // If split response is too large, scale desired size for expected DATAFLOW_API_SIZE_BYTES/2.
    if (serializedSize > apiByteLimit) {
      double expansion = 2 * (double) serializedSize / apiByteLimit;
      long expandedBundleSizeBytes = (long) (desiredBundleSizeBytes * expansion);
      LOG.warn(
          "Splitting source {} into bundles of estimated size {} bytes produced {} bundles, which"
              + " have total serialized size {} bytes. As this is too large for the Google Cloud"
              + " Dataflow API, retrying splitting once with increased desiredBundleSizeBytes {}"
              + " to reduce the number of splits.",
          source,
          desiredBundleSizeBytes,
          bundles.size(),
          serializedSize,
          expandedBundleSizeBytes);
      desiredBundleSizeBytes = expandedBundleSizeBytes;
      bundles = splitAndValidate(source, desiredBundleSizeBytes, options);
      serializedSize =
          DataflowApiUtils.computeSerializedSizeBytes(wrapIntoSourceSplitResponse(bundles));
      LOG.info(
          "Splitting with desiredBundleSizeBytes {} produced {} bundles "
              + "with total serialized size {} bytes",
          desiredBundleSizeBytes,
          bundles.size(),
          serializedSize);
    }

    List<BoundedSource<T>> bundlesBeforeCoalesce = bundles;
    int numBundlesBeforeRebundling = bundles.size();
    // To further reduce size of the response and service-side memory usage, coalesce
    // the sources into numBundlesLimit compressed serialized bundles.
    while (serializedSize > apiByteLimit || bundles.size() > numBundlesLimit) {
      // bundle size constrained by API limit, adds 5% allowance
      int targetBundleSizeApiLimit = (int) (bundles.size() * apiByteLimit / serializedSize * 0.95);
      // bundle size constrained by numBundlesLimit
      int targetBundleSizeBundleLimit = Math.min(numBundlesLimit, bundles.size() - 1);
      int targetBundleSize = Math.min(targetBundleSizeApiLimit, targetBundleSizeBundleLimit);

      if (targetBundleSize <= 1) {
        String message =
            String.format(
                "Unable to coalesce the sources into compressed serialized bundles to satisfy the "
                    + "allowable limit when splitting %s. With %d bundles, total serialized size "
                    + "of %d bytes is still larger than the limit %d. For more information, please "
                    + "check the corresponding FAQ entry at "
                    + "https://cloud.google.com/dataflow/docs/guides/common-errors#boundedsource-objects-splitintobundles",
                source, bundles.size(), serializedSize, apiByteLimit);
        throw new IllegalArgumentException(message);
      }

      bundles = limitNumberOfBundles(bundlesBeforeCoalesce, targetBundleSize);
      serializedSize =
          DataflowApiUtils.computeSerializedSizeBytes(wrapIntoSourceSplitResponse(bundles));
      LOG.warn(
          "Re-bundle source {} into bundles of estimated size {} bytes produced {} bundles.",
          source,
          serializedSize,
          bundles.size());
    }

    SourceOperationResponse response =
        new SourceOperationResponse().setSplit(wrapIntoSourceSplitResponse(bundles));
    long finalResponseSize = DataflowApiUtils.computeSerializedSizeBytes(response);
    LOG.info(
        "Splitting source {} produced {} bundles with total serialized response size {}",
        source,
        bundles.size(),
        finalResponseSize);
    if (finalResponseSize > apiByteLimit) {
      String message =
          String.format(
              "Total size of the BoundedSource objects generated by split() operation is larger "
                  + "than the allowable limit. When splitting %s into bundles of %d bytes "
                  + "it generated %d BoundedSource objects with total serialized size of %d bytes "
                  + "which is larger than the limit %d. "
                  + "For more information, please check the corresponding FAQ entry at "
                  + "https://cloud.google.com/dataflow/docs/guides/common-errors#boundedsource-objects-splitintobundles",
              source,
              desiredBundleSizeBytes,
              numBundlesBeforeRebundling,
              finalResponseSize,
              apiByteLimit);
      throw new IllegalArgumentException(message);
    }
    return response;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static <T> List<BoundedSource<T>> splitAndValidate(
      BoundedSource<T> source, long desiredBundleSizeBytes, PipelineOptions options)
      throws Exception {
    List<BoundedSource<T>> bundles = (List) source.split(desiredBundleSizeBytes, options);
    for (BoundedSource<T> split : bundles) {
      try {
        split.validate();
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "Splitting a valid source produced an invalid source."
                    + "%nOriginal source: %s%nInvalid source: %s",
                source, split),
            e);
      }
    }

    return bundles;
  }

  private static SourceSplitResponse wrapIntoSourceSplitResponse(
      List<? extends BoundedSource<?>> bundles) throws Exception {
    List<DerivedSource> splits = new ArrayList<>(bundles.size());
    for (BoundedSource<?> split : bundles) {
      splits.add(
          new DerivedSource()
              .setDerivationMode("SOURCE_DERIVATION_MODE_INDEPENDENT")
              .setSource(
                  serializeSplitToCloudSource(split)
                      .setDoesNotNeedSplitting(
                          // We purposely set this to false when using SplittableOnlyBoundedSource
                          // to tell the service that we need further splits.
                          !(split instanceof SplittableOnlyBoundedSource))));
    }

    // Return all the splits in the SourceSplitResponse.
    return new SourceSplitResponse()
        .setBundles(splits)
        .setOutcome("SOURCE_SPLIT_OUTCOME_SPLITTING_HAPPENED");
  }

  /** A {@link ReaderFactory.Registrar} for user defined custom sources. */
  @AutoService(ReaderFactory.Registrar.class)
  public static class Registrar implements ReaderFactory.Registrar {

    @Override
    public Map<String, ReaderFactory> factories() {
      Factory factory = new Factory();
      return ImmutableMap.of(
          "org.apache.beam.runners.dataflow.internal.CustomSources", factory,
          "org.apache.beam.runners.dataflow.worker.runners.dataflow.WorkerCustomSources", factory);
    }
  }

  /** Factory to create a {@link WorkerCustomSources} from a Dataflow API source specification. */
  public static class Factory implements ReaderFactory {
    @Override
    public NativeReader<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext)
        throws Exception {
      // The parameter "coder" is deliberately never used. It is an artifact of ReaderFactory:
      // some readers need a coder, some don't (i.e. for some it doesn't even make sense),
      // but ReaderFactory passes it to all readers anyway.
      return WorkerCustomSources.create(spec, options, executionContext);
    }
  }

  public static NativeReader<WindowedValue<?>> create(
      final CloudObject spec,
      final PipelineOptions options,
      DataflowExecutionContext executionContext)
      throws Exception {

    @SuppressWarnings("unchecked")
    final Source<Object> source = (Source<Object>) deserializeFromCloudSource(spec);

    if (source instanceof BoundedSource) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      NativeReader<WindowedValue<?>> reader =
          (NativeReader)
              new NativeReader<WindowedValue<Object>>() {
                @Override
                public NativeReaderIterator<WindowedValue<Object>> iterator() throws IOException {
                  return new BoundedReaderIterator<>(
                      ((BoundedSource<Object>) source).createReader(options));
                }
              };
      return reader;
    } else if (source instanceof UnboundedSource) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      NativeReader<WindowedValue<?>> reader =
          (NativeReader)
              new UnboundedReader<Object>(
                  options, spec, (StreamingModeExecutionContext) executionContext);
      return reader;
    } else {
      throw new IllegalArgumentException("Unexpected source kind: " + source.getClass());
    }
  }

  private static final ByteString firstSplitKey = ByteString.copyFromUtf8("0000000000000001");

  public static boolean isFirstUnboundedSourceSplit(ByteString splitKey) {
    return splitKey.equals(firstSplitKey);
  }

  /** {@link NativeReader} for reading from {@link UnboundedSource UnboundedSources}. */
  private static class UnboundedReader<T>
      extends NativeReader<WindowedValue<ValueWithRecordId<T>>> {
    private final PipelineOptions options;
    private final CloudObject spec;
    private final StreamingModeExecutionContext context;

    UnboundedReader(
        PipelineOptions options, CloudObject spec, StreamingModeExecutionContext context) {
      this.options = options;
      this.spec = spec;
      this.context = context;
    }

    @Override
    @SuppressWarnings("unchecked")
    public NativeReaderIterator<WindowedValue<ValueWithRecordId<T>>> iterator() throws IOException {
      UnboundedSource.UnboundedReader<T> reader =
          (UnboundedSource.UnboundedReader<T>) context.getCachedReader();
      final boolean started = reader != null;
      if (reader == null) {
        String key = context.getSerializedKey().toStringUtf8();
        // Key is expected to be a zero-padded integer representing the split index.
        int splitIndex = Integer.parseInt(key.substring(0, 16), 16) - 1;

        UnboundedSource<T, UnboundedSource.CheckpointMark> splitSource = parseSource(splitIndex);

        UnboundedSource.@Nullable CheckpointMark checkpoint = null;
        if (splitSource.getCheckpointMarkCoder() != null) {
          checkpoint = context.getReaderCheckpoint(splitSource.getCheckpointMarkCoder());
        }

        reader = splitSource.createReader(options, checkpoint);
      }

      context.setActiveReader(reader);

      return new UnboundedReaderIterator<>(reader, context, started, options);
    }

    @Override
    public boolean supportsRestart() {
      return true;
    }

    @SuppressWarnings("unchecked")
    private UnboundedSource<T, UnboundedSource.CheckpointMark> parseSource(int index) {
      List<String> serializedSplits = null;
      try {
        serializedSplits = getStrings(spec, SERIALIZED_SOURCE_SPLITS, null);
      } catch (Exception e) {
        throw new RuntimeException("Parsing serialized source splits failed: ", e);
      }
      checkArgument(serializedSplits != null, "UnboundedSource object did not contain splits");
      checkArgument(
          index < serializedSplits.size(),
          "UnboundedSource splits contained too few splits.  Requested index was %s, size was %s",
          index,
          serializedSplits.size());
      Object rawSource =
          deserializeFromByteArray(
              decodeBase64(serializedSplits.get(index)), "UnboundedSource split");
      if (!(rawSource instanceof UnboundedSource)) {
        throw new IllegalArgumentException("Expected UnboundedSource, got " + rawSource.getClass());
      }
      return (UnboundedSource<T, UnboundedSource.CheckpointMark>) rawSource;
    }
  }

  /**
   * This is a bounded source that doesn't know how to read data. It is used to encompass several
   * splits to workaround Dataflow API limits. It is able to achieve this goal by being compressed
   * which leverages the fact that this object stores several splits so compression works across
   * these splits. Empirically this lets us scale out the amount of initial splits the user returned
   * by a few orders of magnitude.
   *
   * <p>TODO: Replace with a concat custom source once one is available or deprecate in favor of <a
   * href="https://issues.apache.org/jira/browse/BEAM-65">splittable DoFns</a>.
   */
  @VisibleForTesting
  static final class SplittableOnlyBoundedSource<T> extends BoundedSource<T> {
    private final List<? extends BoundedSource<T>> boundedSources;

    private SplittableOnlyBoundedSource(List<? extends BoundedSource<T>> boundedSources) {
      this.boundedSources = ImmutableList.copyOf(boundedSources);
    }

    @Override
    public List<? extends BoundedSource<T>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return boundedSources;
    }

    @Override
    public long getEstimatedSizeBytes(final PipelineOptions options) throws Exception {
      List<Callable<Long>> callables = new ArrayList<>(boundedSources.size());
      for (final BoundedSource<T> source : boundedSources) {
        callables.add(() -> source.getEstimatedSizeBytes(options));
      }

      long sum = 0L;
      for (Future<Long> result :
          options.as(DataflowPipelineOptions.class).getExecutorService().invokeAll(callables)) {
        sum += result.get();
      }
      return sum;
    }

    @Override
    public BoundedSource.BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      throw new UnsupportedOperationException(
          "SplittableOnlyBoundedSource only supports splitting.");
    }

    @Override
    public void validate() {
      for (BoundedSource<T> boundedSource : boundedSources) {
        boundedSource.validate();
      }
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return boundedSources.get(0).getDefaultOutputCoder();
    }
  }

  private static <T> List<BoundedSource<T>> limitNumberOfBundles(
      List<BoundedSource<T>> bundles, int maxBundles) {
    List<BoundedSource<T>> splittableBoundedSources = new ArrayList<>();
    // Greedily create SplittableOnlyBoundedSources with powers of "maxBundles" to
    // minimize the number of splits required to be done.

    // Find the largest power of "maxBundles" that is strictly less than the number
    // of elements. We use a long here to not have to worry about overflowing 2^32 during
    // comparison.
    int numElementsToPutIntoBundle = maxBundles;
    while (bundles.size() > (long) numElementsToPutIntoBundle * maxBundles) {
      numElementsToPutIntoBundle *= maxBundles;
    }

    // Insert as many full groups of bundles of the largest size that we can
    int startIndex = 0;
    // Compute the remaining capacity if we were to fit all the rest of the bundles with
    // smaller splits.
    int remainingCapacity =
        numElementsToPutIntoBundle
            / maxBundles
            * (maxBundles - splittableBoundedSources.size() - 1);
    for (;
        startIndex < bundles.size() - numElementsToPutIntoBundle - remainingCapacity;
        startIndex += numElementsToPutIntoBundle) {
      splittableBoundedSources.add(
          new SplittableOnlyBoundedSource<>(
              bundles.subList(startIndex, startIndex + numElementsToPutIntoBundle)));
      remainingCapacity =
          numElementsToPutIntoBundle
              / maxBundles
              * (maxBundles - splittableBoundedSources.size() - 1);
    }

    // We compute how many elements we should place into the next bundle based upon how many
    // we can fit of the smaller size in the remaining spots.
    splittableBoundedSources.add(
        new SplittableOnlyBoundedSource<>(
            bundles.subList(startIndex, bundles.size() - remainingCapacity)));
    startIndex = bundles.size() - remainingCapacity;

    // Use the smaller bundle size to fill in the remaining bundles.
    numElementsToPutIntoBundle /= maxBundles;
    for (; startIndex < bundles.size(); startIndex += numElementsToPutIntoBundle) {
      if (numElementsToPutIntoBundle == 1) {
        splittableBoundedSources.add(bundles.get(startIndex));
      } else {
        splittableBoundedSources.add(
            new SplittableOnlyBoundedSource<>(
                bundles.subList(startIndex, startIndex + numElementsToPutIntoBundle)));
      }
    }

    return splittableBoundedSources;
  }

  @VisibleForTesting
  static Source<?> deserializeFromCloudSource(Map<String, Object> spec) throws Exception {
    Source<?> source =
        (Source<?>)
            deserializeFromByteArray(
                Base64.decodeBase64(getString(spec, SERIALIZED_SOURCE)), "Source");
    try {
      source.validate();
    } catch (Exception e) {
      LOG.error("Invalid source: {}", source, e);
      throw e;
    }
    return source;
  }

  @VisibleForTesting
  static class BoundedReaderIterator<T>
      extends NativeReader.NativeReaderIterator<WindowedValue<T>> {
    private BoundedSource.BoundedReader<T> reader;

    private BoundedReaderIterator(BoundedSource.BoundedReader<T> reader) {
      this.reader = reader;
    }

    @Override
    public boolean start() throws IOException {
      try {
        return reader.start();
      } catch (Exception e) {
        throw new IOException(
            "Failed to start reading from source: " + reader.getCurrentSource(), e);
      }
    }

    @Override
    public boolean advance() throws IOException {
      try {
        return reader.advance();
      } catch (Exception e) {
        throw new IOException(
            "Failed to advance reader of source: " + reader.getCurrentSource(), e);
      }
    }

    @Override
    public WindowedValue<T> getCurrent() throws NoSuchElementException {
      return WindowedValue.timestampedValueInGlobalWindow(
          reader.getCurrent(), reader.getCurrentTimestamp());
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @VisibleForTesting
    static @Nullable ReportedParallelism longToParallelism(long value) {
      if (value >= 0) {
        return new ReportedParallelism().setValue(Double.valueOf(value));
      } else {
        return null;
      }
    }

    /**
     * Testable helper for {@link #getProgress}. Sets fraction consumed, parallelism consumed, and
     * parallelism remaining while also being fault tolerant.
     */
    @VisibleForTesting
    static ApproximateReportedProgress getReaderProgress(BoundedSource.BoundedReader<?> reader) {
      ApproximateReportedProgress progress = new ApproximateReportedProgress();
      // Fraction consumed
      try {
        Double fractionConsumed = reader.getFractionConsumed();
        if (fractionConsumed != null) {
          progress.setFractionConsumed(fractionConsumed);
        }
      } catch (Throwable t) {
        LOG.warn("Error estimating fraction consumed from reader {}", reader, t);
      }
      // Parallelism consumed
      try {
        ReportedParallelism parallelism = longToParallelism(reader.getSplitPointsConsumed());
        if (parallelism != null) {
          progress.setConsumedParallelism(parallelism);
        }
      } catch (Throwable t) {
        LOG.warn("Error estimating consumed parallelism from reader {}", reader, t);
      }
      // Parallelism remaining
      try {
        ReportedParallelism parallelism = longToParallelism(reader.getSplitPointsRemaining());
        if (parallelism != null) {
          progress.setRemainingParallelism(parallelism);
        }
      } catch (Throwable t) {
        LOG.warn("Error estimating remaining parallelism from reader {}", reader, t);
      }
      return progress;
    }

    @Override
    public NativeReader.Progress getProgress() {
      // Delegate to testable helper.
      return SourceTranslationUtils.cloudProgressToReaderProgress(getReaderProgress(reader));
    }

    @Override
    public NativeReader.DynamicSplitResult requestDynamicSplit(
        NativeReader.DynamicSplitRequest request) {
      ApproximateSplitRequest stopPosition =
          SourceTranslationUtils.splitRequestToApproximateSplitRequest(request);
      Double fractionConsumed = stopPosition.getFractionConsumed();
      if (fractionConsumed == null) {
        // Only truncating at a fraction is currently supported.
        LOG.info(
            "Rejecting split request because custom sources only support splits at fraction: {}",
            stopPosition);
        return null;
      }
      BoundedSource<T> original = reader.getCurrentSource();
      BoundedSource<T> residual = reader.splitAtFraction(fractionConsumed);
      if (residual == null) {
        LOG.info("Rejecting split request because custom reader returned null residual source.");
        return null;
      }
      // Try to catch some potential subclass implementation errors early.
      BoundedSource<T> primary = reader.getCurrentSource();
      if (original == primary) {
        throw new IllegalStateException(
            "Successful split did not change the current source: primary is identical to original"
                + " (Source objects MUST be immutable): "
                + primary);
      }
      if (original == residual) {
        throw new IllegalStateException(
            "Successful split did not change the current source: residual is identical to original"
                + " (Source objects MUST be immutable): "
                + residual);
      }
      try {
        primary.validate();
      } catch (Exception e) {
        throw new IllegalStateException(
            "Successful split produced an illegal primary source. "
                + "\nOriginal: "
                + original
                + "\nPrimary: "
                + primary
                + "\nResidual: "
                + residual);
      }
      try {
        residual.validate();
      } catch (Exception e) {
        throw new IllegalStateException(
            "Successful split produced an illegal residual source. "
                + "\nOriginal: "
                + original
                + "\nPrimary: "
                + primary
                + "\nResidual: "
                + residual);
      }
      return new BoundedSourceSplit<T>(primary, residual);
    }

    @Override
    public double getRemainingParallelism() {
      return Double.NaN;
    }
  }

  private static class UnboundedReaderIterator<T>
      extends NativeReader.NativeReaderIterator<WindowedValue<ValueWithRecordId<T>>> {
    // Do not close reader. The reader is cached in StreamingModeExecutionContext.readerCache, and
    // will be reused until the cache is evicted, expired or invalidated.
    // See UnboundedReader#iterator().
    private final UnboundedSource.UnboundedReader<T> reader;
    private final StreamingModeExecutionContext context;
    private final boolean started;
    private final Instant endTime;
    private final int maxElems;
    private final FluentBackoff backoffFactory;
    private int elemsRead = 0;

    private UnboundedReaderIterator(
        UnboundedSource.UnboundedReader<T> reader,
        StreamingModeExecutionContext context,
        boolean started,
        PipelineOptions options) {
      this.reader = reader;
      this.context = context;
      this.started = started;
      DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
      long maxReadTimeMs = debugOptions.getUnboundedReaderMaxReadTimeMs();
      this.endTime = Instant.now().plus(Duration.millis(maxReadTimeMs));
      this.maxElems = debugOptions.getUnboundedReaderMaxElements();
      this.backoffFactory =
          FluentBackoff.DEFAULT
              .withInitialBackoff(Duration.millis(10))
              .withMaxCumulativeBackoff(
                  Duration.millis(debugOptions.getUnboundedReaderMaxWaitForElementsMs()));
    }

    @Override
    public boolean start() throws IOException {
      if (started) {
        // This is a reader that has been restored from the unbounded reader cache.
        // It has already been started, so this call to start() should delegate
        // to advance() instead.
        return advance();
      }
      try {
        if (!reader.start()) {
          return false;
        }
      } catch (Exception e) {
        throw new IOException(
            "Failed to start reading from source: " + reader.getCurrentSource(), e);
      }
      elemsRead++;
      return true;
    }

    @Override
    public boolean advance() throws IOException {
      // Limits are placed on how much data we allow to return, how long we process the input
      // before checkpointing and how long we block for input to be available.  This ensures
      // that there are regular checkpoints and that state does not become too large.
      BackOff backoff = backoffFactory.backoff();
      while (true) {
        if (elemsRead >= maxElems
            || Instant.now().isAfter(endTime)
            || context.isSinkFullHintSet()
            || context.workIsFailed()) {
          return false;
        }
        try {
          if (reader.advance()) {
            elemsRead++;
            return true;
          }
        } catch (Exception e) {
          throw new IOException("Failed to advance source: " + reader.getCurrentSource(), e);
        }
        long nextBackoff = backoff.nextBackOffMillis();
        if (nextBackoff == BackOff.STOP) {
          return false;
        }
        Uninterruptibles.sleepUninterruptibly(nextBackoff, TimeUnit.MILLISECONDS);
      }
    }

    @Override
    public WindowedValue<ValueWithRecordId<T>> getCurrent() throws NoSuchElementException {
      WindowedValue<T> result =
          WindowedValue.timestampedValueInGlobalWindow(
              reader.getCurrent(), reader.getCurrentTimestamp());
      return result.withValue(
          new ValueWithRecordId<>(result.getValue(), reader.getCurrentRecordId()));
    }

    @Override
    public void close() {
      // Don't close reader.
    }

    @Override
    public NativeReader.Progress getProgress() {
      return null;
    }

    @Override
    public NativeReader.DynamicSplitResult requestDynamicSplit(
        NativeReader.DynamicSplitRequest request) {
      return null;
    }

    @Override
    public double getRemainingParallelism() {
      return Double.NaN;
    }
  }
}
