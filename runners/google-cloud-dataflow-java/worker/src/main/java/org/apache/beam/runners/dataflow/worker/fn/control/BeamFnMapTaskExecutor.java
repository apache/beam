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
package org.apache.beam.runners.dataflow.worker.fn.control;

import com.google.api.services.dataflow.model.CounterUpdate;
import io.opencensus.common.Scope;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.fn.data.RemoteGrpcPortWriteOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.DynamicSplitRequest;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.DynamicSplitResult;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.Progress;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.WorkExecutor;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link WorkExecutor} that processes a list of {@link Operation}s.
 *
 * <p>Note that this executor is meant to be used with the Fn API. Several of the methods to request
 * splitting, checkpointing, work progress are unimplemented.
 */
public class BeamFnMapTaskExecutor extends DataflowMapTaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnMapTaskExecutor.class);

  private final ProgressTracker progressTracker;

  private BeamFnMapTaskExecutor(
      List<Operation> operations,
      CounterSet counters,
      ExecutionStateTracker executionStateTracker) {
    super(operations, counters, executionStateTracker);
    this.progressTracker = createProgressTracker();
    LOG.debug("Creating BeamFnMapTaskExecutor");
  }

  /**
   * Creates a new MapTaskExecutor.
   *
   * @param operations the operations of the map task, in order of execution
   */
  public static BeamFnMapTaskExecutor forOperations(
      List<Operation> operations, ExecutionStateTracker executionStateTracker) {
    return new BeamFnMapTaskExecutor(operations, new CounterSet(), executionStateTracker);
  }

  /**
   * Creates a new MapTaskExecutor with a shared set of counters with its creator.
   *
   * @param operations the operations of the map task, in order of execution
   * @param counters a set of system counters associated with operations, which may get extended
   *     during execution
   */
  public static BeamFnMapTaskExecutor withSharedCounterSet(
      List<Operation> operations,
      CounterSet counters,
      ExecutionStateTracker executionStateTracker) {
    return new BeamFnMapTaskExecutor(operations, counters, executionStateTracker);
  }

  @Override
  public void execute() throws Exception {
    Tracer tracer = Tracing.getTracer();
    SpanBuilder builder = tracer.spanBuilder("MapTaskExecutor.Span").setRecordEvents(true);

    // Start the progress tracker before execution (which blocks until execution is finished).
    try (Scope unused = builder.startScopedSpan();
        AutoCloseable unused2 = progressTrackerCloseable(progressTracker)) {
      tracer.getCurrentSpan().addAnnotation("About to execute");
      super.execute();
      tracer.getCurrentSpan().addAnnotation("Done with execute");
    }
  }

  private AutoCloseable progressTrackerCloseable(ProgressTracker progressTracker) {
    progressTracker.start();
    return () -> progressTracker.stop();
  }

  @Override
  public @Nullable Progress getWorkerProgress() throws Exception {
    return progressTracker.getWorkerProgress();
  }

  /**
   * {@inheritDoc}
   *
   * @return User-defined Beam metrics reported over the Fn API.
   */
  @Override
  public Iterable<CounterUpdate> extractMetricUpdates() {
    List<CounterUpdate> result = progressTracker.extractCounterUpdates();

    if ((result != null) && (result.size() > 0)) {
      return result;
    }

    // todo(BEAM-6189): Remove this fallback once Metrics is deprecated from SDKs.
    MetricUpdates updates = progressTracker.extractMetricUpdates();

    Iterable<CounterUpdate> deprecatedMetrics =
        Iterables.concat(
            StreamSupport.stream(updates.counterUpdates().spliterator(), false)
                .map(
                    update ->
                        MetricsToCounterUpdateConverter.fromCounter(
                            update.getKey(), true, update.getUpdate()))
                .collect(Collectors.toList()),
            StreamSupport.stream(updates.distributionUpdates().spliterator(), false)
                .map(
                    update ->
                        MetricsToCounterUpdateConverter.fromDistribution(
                            update.getKey(), true, update.getUpdate()))
                .collect(Collectors.toList()));

    return deprecatedMetrics;
  }

  @Override
  public @Nullable DynamicSplitResult requestCheckpoint() throws Exception {
    return progressTracker.requestCheckpoint();
  }

  @Override
  public @Nullable DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest)
      throws Exception {
    return progressTracker.requestDynamicSplit(splitRequest);
  }

  @Override
  public ReadOperation getReadOperation() throws Exception {
    // TODO: Remove streaming Dataflow's reliance on access to the "ReadOperation".
    for (Operation operation : operations) {
      if (operation instanceof ReadOperation) {
        return (ReadOperation) operation;
      }
    }
    throw new IllegalStateException(String.format("ReadOperation not found in %s", operations));
  }

  private interface ProgressTracker {

    public @Nullable Progress getWorkerProgress() throws Exception;

    /**
     * Returns an metric updates accumulated since the last call to {@link #extractMetricUpdates()}.
     */
    @Deprecated
    public MetricUpdates extractMetricUpdates();

    public List<CounterUpdate> extractCounterUpdates();

    public @Nullable DynamicSplitResult requestCheckpoint() throws Exception;

    public @Nullable DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest)
        throws Exception;

    public default void start() {}

    public default void stop() {}
  }

  private static class NullProgressTracker implements ProgressTracker {
    @Nullable
    @Override
    public Progress getWorkerProgress() {
      return null;
    }

    @Override
    public MetricUpdates extractMetricUpdates() {
      return MetricUpdates.EMPTY;
    }

    @Override
    public List<CounterUpdate> extractCounterUpdates() {
      return Collections.emptyList();
    }

    @Nullable
    @Override
    public DynamicSplitResult requestCheckpoint() {
      return null;
    }

    @Nullable
    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      return null;
    }
  }

  private static class ReadOperationProgressTracker implements ProgressTracker {
    final ReadOperation readOperation;

    public ReadOperationProgressTracker(ReadOperation readOperation) {
      this.readOperation = readOperation;
    }

    @Nullable
    @Override
    public Progress getWorkerProgress() throws Exception {
      return readOperation.getProgress();
    }

    @Override
    public MetricUpdates extractMetricUpdates() {
      return MetricUpdates.EMPTY;
    }

    @Override
    public List<CounterUpdate> extractCounterUpdates() {
      return Collections.emptyList();
    }

    @Nullable
    @Override
    public DynamicSplitResult requestCheckpoint() throws Exception {
      return readOperation.requestCheckpoint();
    }

    @Nullable
    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest)
        throws Exception {
      return readOperation.requestDynamicSplit(splitRequest);
    }
  }

  @VisibleForTesting
  /*package*/ static class SingularProcessBundleProgressTracker implements ProgressTracker {
    private static final int MAX_DATA_POINTS = 1000;

    private final ReadOperation readOperation;
    private final RemoteGrpcPortWriteOperation grpcWriteOperation;
    private final RegisterAndProcessBundleOperation bundleProcessOperation;
    private static final long progressUpdatePeriodMs =
        ReadOperation.DEFAULT_PROGRESS_UPDATE_PERIOD_MS;
    private int progressErrors;

    private final Interpolator<Progress> progressInterpolator;

    private final AtomicReference<Progress> latestProgress = new AtomicReference<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> nextProgressFuture;
    private final Consumer<Integer> grpcWriteOperationElementsProcessed;

    private List<CounterUpdate> counterUpdates = new ArrayList<>();

    private final Map<MetricKey, MetricUpdate<Long>> deprecatedCounterUpdates;
    private final Map<MetricKey, MetricUpdate<DistributionData>> deprecatedDistributionUpdates;
    private final Map<MetricKey, MetricUpdate<GaugeData>> deprecatedGaugeUpdates;

    public SingularProcessBundleProgressTracker(
        ReadOperation readOperation,
        RemoteGrpcPortWriteOperation grpcWriteOperation,
        RegisterAndProcessBundleOperation bundleProcessOperation) {
      this.readOperation = readOperation;
      this.grpcWriteOperation = grpcWriteOperation;
      this.bundleProcessOperation = bundleProcessOperation;
      this.grpcWriteOperationElementsProcessed = grpcWriteOperation.processedElementsConsumer();
      this.progressInterpolator =
          new Interpolator<Progress>(MAX_DATA_POINTS) {
            @Override
            protected Progress interpolate(Progress prev, Progress next, double fraction) {
              return prev;
            }
          };
      this.deprecatedCounterUpdates = new HashMap<>();
      this.deprecatedDistributionUpdates = new HashMap<>();
      this.deprecatedGaugeUpdates = new HashMap<>();
    }

    private void periodicProgressUpdate() {
      updateProgress();
    }

    @VisibleForTesting
    void updateProgress() {
      try {
        if (bundleProcessOperation.hasFailed()) {
          grpcWriteOperation.abortWait();
        }

        ProcessBundleProgressResponse processBundleProgressResponse =
            MoreFutures.get(bundleProcessOperation.getProcessBundleProgress());

        final List<MonitoringInfo> monitoringInfosList =
            processBundleProgressResponse.getMonitoringInfosList();

        updateMetrics(monitoringInfosList);

        double elementsConsumed =
            bundleProcessOperation.getInputElementsConsumed(monitoringInfosList);
        grpcWriteOperationElementsProcessed.accept((int) elementsConsumed);
        progressInterpolator.addPoint(
            grpcWriteOperation.getElementsSent(), readOperation.getProgress());
        latestProgress.set(progressInterpolator.interpolateAndPurge(elementsConsumed));
        progressErrors = 0;
      } catch (Exception exn) {
        if (!isTransientProgressError(exn.getMessage())) {
          grpcWriteOperationElementsProcessed.accept(-1); // Not supported.
          progressErrors++;
          // JRH schedules progress report every 5 sec. So wait for 5 mins to show the log.
          if (progressErrors == 60) {
            String bundleId = bundleProcessOperation.getCurrentProcessBundleInstructionId();
            if (bundleId == null) {
              LOG.info(
                  String.format(
                      "Runner failed to get progress from SDK because current bundle has been "
                          + "finished or not start in SDK yet"));
            } else {
              LOG.warn(
                  String.format(
                      "Runner has failed to get progress from SDK for %s times for bundle %s. "
                          + "Possibly caused by SDK doesn't support progress report.",
                      progressErrors, bundleId),
                  exn);
            }
          }
        }

        try {
          latestProgress.set(
              progressInterpolator.interpolate(grpcWriteOperation.getElementsSent()));
        } catch (IllegalStateException exn2) {
          // This can happen if the operation has not yet been started; don't update
          // the progress in this case.
        } catch (Exception e) {
          LOG.error("Exception in catch of updateProgress():", e);
          throw e;
        }
      }
    }

    /**
     * Updates internal metrics state from provided monitoringInfos list.
     *
     * @param monitoringInfos Usually received from FnApi.
     */
    private void updateMetrics(List<MonitoringInfo> monitoringInfos) {
      List<MonitoringInfo> monitoringInfosCopy = new ArrayList<>(monitoringInfos);

      List<MonitoringInfo> misToFilter =
          bundleProcessOperation.findIOPCollectionMonitoringInfos(monitoringInfos);
      monitoringInfosCopy.removeAll(misToFilter);

      final MonitoringInfoToCounterUpdateTransformer monitoringInfoToCounterUpdateTransformer =
          new FnApiMonitoringInfoToCounterUpdateTransformer(
              this.bundleProcessOperation.getPtransformIdToUserStepContext(),
              this.bundleProcessOperation.getPCollectionIdToNameContext());

      counterUpdates =
          monitoringInfosCopy.stream()
              .map(monitoringInfoToCounterUpdateTransformer::transform)
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
    }

    @Nullable
    @Override
    public Progress getWorkerProgress() throws Exception {
      return latestProgress.get();
    }

    @Override
    public List<CounterUpdate> extractCounterUpdates() {
      return counterUpdates;
    }

    @Override
    public MetricUpdates extractMetricUpdates() {
      Map<MetricKey, MetricUpdate<Long>> snapshotCounterUpdates = deprecatedCounterUpdates;
      Map<MetricKey, MetricUpdate<DistributionData>> snapshotDistributionUpdates =
          deprecatedDistributionUpdates;
      Map<MetricKey, MetricUpdate<GaugeData>> snapshotGaugeUpdates = deprecatedGaugeUpdates;
      return MetricUpdates.create(
          snapshotCounterUpdates.values(),
          snapshotDistributionUpdates.values(),
          snapshotGaugeUpdates.values());
    }

    @Nullable
    @Override
    public DynamicSplitResult requestCheckpoint() throws Exception {
      // TODO: Implement checkpointing
      return null;
    }

    @Nullable
    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest)
        throws Exception {
      return readOperation.requestDynamicSplit(splitRequest);
    }

    @Override
    public void start() {
      LOG.debug("Starting BeamFnMapTaskExecutor, launching progress thread");
      progressErrors = 0;
      nextProgressFuture =
          scheduler.scheduleAtFixedRate(
              this::periodicProgressUpdate,
              progressUpdatePeriodMs,
              progressUpdatePeriodMs,
              TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
      LOG.debug("Stopping BeamFnMapTaskExecutor, grabbing final metric updates");
      nextProgressFuture.cancel(true);
      try {
        nextProgressFuture.get();
      } catch (CancellationException | ExecutionException | InterruptedException exn) {
        // expected
      }
      progressInterpolator.clear();
      latestProgress.set(null);

      // Set final metrics to precisely the values in this update. This should overwrite, not
      // be combined with, all prior updates.
      counterUpdates.clear();
      deprecatedCounterUpdates.clear();
      deprecatedDistributionUpdates.clear();
      deprecatedGaugeUpdates.clear();
      try {
        updateMetrics(MoreFutures.get(bundleProcessOperation.getFinalMonitoringInfos()));
      } catch (ExecutionException | InterruptedException exn) {
        LOG.info("Failed to get final metrics for bundle", exn);
      }
    }
  }

  /**
   * Interpolates values between a dynamic set of known data points.
   *
   * <p>Performs piecewise linear interpolation, extrapolating values beyond known bounds by giving
   * the corresponding minimum or maximum known value.
   *
   * <p>Data must be added in a monotonically increasing manner, and may be purged in a similar
   * fashon. This makes this class suitable for performing interpolation over a sliding window.
   * Regardless of how many points are added, this class only uses a bounded amount of memory.
   */
  @NotThreadSafe
  @VisibleForTesting
  /*package*/ abstract static class Interpolator<T> {

    private double[] xs;
    private T[] ys;
    private int numPoints;
    private double lowerBound = Double.NEGATIVE_INFINITY;

    public Interpolator(int maxDataPoints) {
      Preconditions.checkArgument(maxDataPoints >= 2);
      xs = new double[maxDataPoints];
      Arrays.fill(xs, Double.NaN);
      ys = (T[]) new Object[maxDataPoints];
      numPoints = 0;
    }

    public void addPoint(double x, T y) {
      Preconditions.checkArgument(
          numPoints == 0 || xs[numPoints - 1] <= x,
          "Data must be added in monotonicaly increasing order.");
      if (numPoints >= xs.length) {
        purgeInternal();
      }
      xs[numPoints] = x;
      ys[numPoints] = y;
      numPoints += 1;
    }

    public T interpolate(double x) {
      if (numPoints == 0) {
        return null;
      } else if (x <= xs[0]) {
        return ys[0];
      } else if (xs[numPoints - 1] <= x) {
        return ys[numPoints - 1];
      } else {
        int index = Arrays.binarySearch(xs, 0, numPoints, x);
        if (index >= 0) {
          return ys[index];
        } else {
          int after = -index - 1;
          int before = after - 1;
          return interpolate(ys[before], ys[after], (x - xs[before]) / (xs[after] - xs[before]));
        }
      }
    }

    public void purgeUpTo(double x) {
      lowerBound = x;
    }

    public T interpolateAndPurge(double x) {
      purgeUpTo(x);
      return interpolate(x);
    }

    public void clear() {
      Arrays.fill(xs, Double.NaN); // as a precaution
      Arrays.fill(ys, null); // free up any references to positions
      numPoints = 0;
    }

    /** Returns a value fraction of the way from prev to next. */
    protected abstract T interpolate(T prev, T next, double fraction);

    private void purgeInternal() {
      assert numPoints > 2;
      if (xs[1] < lowerBound) {
        // If there's uneeded elements at the front of our array, shift everything over
        // to make more room at the end.
        int start = Arrays.binarySearch(xs, 0, numPoints, lowerBound);
        // If positive, start is the index of the element that matches the location
        // of lowerBound in the array. If negative, it can be transformed into the index
        // of where it would be inserted. We recover that value. Note that start
        // cannot be 0 since we checked xs[1] against lowerBound.
        if (start < 0) {
          start = -start - 1;
        }
        // We want to preserve a single value that is lower than lowerBound for interpolation
        // purposes.
        start--;
        numPoints -= start;
        System.arraycopy(xs, start, xs, 0, numPoints);
        System.arraycopy(ys, start, ys, 0, numPoints);
      } else {
        // There's no extra room, we must throw away some data.
        // Interpolate a set of data points half as dense but over the same range.
        double[] newXs = new double[xs.length];
        T[] newYs = (T[]) new Object[ys.length];
        newXs[0] = xs[0];
        newYs[0] = ys[0];
        double dx = (xs[numPoints - 1] - xs[0]) / (numPoints / 2.0);
        for (int i = 1; i < numPoints / 2; i++) {
          double x = xs[0] + i * dx;
          newXs[i] = x;
          newYs[i] = interpolate(x);
        }
        newXs[numPoints / 2] = xs[numPoints - 1];
        newYs[numPoints / 2] = ys[numPoints - 1];
        xs = newXs;
        ys = newYs;
        numPoints = numPoints / 2 + 1;
      }
      Arrays.fill(xs, numPoints, ys.length, Double.NaN); // as a precaution
      Arrays.fill(ys, numPoints, ys.length, null); // free up any references to positions
    }
  }

  private ProgressTracker createProgressTracker() {
    ReadOperation readOperation;
    RemoteGrpcPortWriteOperation grpcWriteOperation;
    RegisterAndProcessBundleOperation bundleProcessOperation;

    try {
      readOperation = getReadOperation();
    } catch (Exception exn) {
      readOperation = null;
      LOG.info("Unable to get read operation.", exn);
      return new NullProgressTracker();
    }

    // If there is a exactly one of each of RemoteGrpcPortWriteOperation and
    // RegisterAndProcessBundleOperation we know they have the right topology.
    try {
      grpcWriteOperation =
          Iterables.getOnlyElement(
              Iterables.filter(operations, RemoteGrpcPortWriteOperation.class));
      bundleProcessOperation =
          Iterables.getOnlyElement(
              Iterables.filter(operations, RegisterAndProcessBundleOperation.class));
    } catch (IllegalArgumentException | NoSuchElementException exn) {
      // TODO: Handle more than one sdk worker processing a single bundle.
      grpcWriteOperation = null;
      bundleProcessOperation = null;
      LOG.debug("Does not have exactly one grpcWRite and bundleProcess operation.", exn);
    }

    if (grpcWriteOperation != null && bundleProcessOperation != null) {
      return new SingularProcessBundleProgressTracker(
          readOperation, grpcWriteOperation, bundleProcessOperation);
    } else {
      return new ReadOperationProgressTracker(readOperation);
    }
  }

  /** Whether the given error is likely to go away (e.g. the bundle has not started). */
  private static boolean isTransientProgressError(String msg) {
    return msg != null
        && (msg.contains("Process bundle request not yet scheduled")
            || msg.contains("Unknown process bundle instruction")
            || msg.contains("unstarted operation"));
  }
}
