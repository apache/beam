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
package org.apache.beam.sdk.transforms;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that wraps a dofn and converts it from one which process elements synchronously to one
 * which processes them asynchronously.
 *
 * <p>For synchronous dofns the default settings mean that many (100s) of elements will be processed
 * in parallel and that processing an element will block all other work on that key. In addition
 * runners are optimized for latencies less than a few seconds and longer operations can result in
 * high retry rates. Async should be considered when the default parallelism is not correct and/or
 * items are expected to take longer than a few seconds to process.
 *
 * <p>/* NOTE: 1) The wrapped syncFn REQUIRES thread-safety if BOTH parallelism > 1 and the DoFn is
 * stateful. 2) Tagged output multi-outputs are unsupported. 3) StartBundle/finishBundle are invoked
 * per element so any batching or aggregation logic will not behave as expected.
 */
public class AsyncWrapper<K, InputT, OutputT> extends DoFn<KV<K, InputT>, OutputT> {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncWrapper.class);

  private static final int DEFAULT_MIN_BUFFER_CAPACITY = 10;
  private static final int DEFAULT_TIMEOUT_SEC = 1;
  private static final int DEFAULT_MAX_WAIT_TIME_MS = 500;
  private static final int TEARDOWN_AWAIT_SEC = 5;
  private static final int INITIAL_BACKOFF_SLEEP_MS = 10;
  private static final int BACKPRESSURE_LOG_THRESHOLD_MS = 10000;
  private static final double HASH_MODULO_LIMIT = 1000000.0;
  private static final double MS_PER_SEC = 1000.0;

  @StateId("to_process")
  private final StateSpec<BagState<KV<K, InputT>>> toProcessSpec;

  @TimerId("timer")
  private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  private final DoFn<InputT, OutputT> syncFn;
  private final int parallelism;
  private final Duration timerFrequency;
  private final int maxItemsToBuffer;
  private final Duration timeout;
  private final Duration maxWaitTime;
  private final SerializableFunction<InputT, Object> idFn;
  private final boolean useThreadPool;
  private final String uuid;

  private transient volatile @Nullable PipelineOptions pipelineOptions;

  // Shared JVM-Wide States (Static Registries)
  // Map-backed registry holding shared resources across serialized worker instances. Since runners
  // clone DoFn instances on the same worker node, static maps ensure safe JVM-wide resource reuse.
  private static final ConcurrentHashMap<String, ExecutorService> pool = new ConcurrentHashMap<>();
  // activeElements (processingElements) is global JVM memory (all keys)
  private static final ConcurrentHashMap<String, ConcurrentHashMap<Object, InFlightElement<?>>>
      processingElements = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, AtomicInteger> itemsInBuffer =
      new ConcurrentHashMap<>();
  // Reference counts for cloned instances sharing the same UUID. Coordinates safe,
  // leak-free thread pool shutdown during teardown without crashing active sibling clones.
  private static final ConcurrentHashMap<String, AtomicInteger> refCounts =
      new ConcurrentHashMap<>();

  // If contention becomes a bottleneck, this can be replaced with per-uuid locks
  // in a ConcurrentHashMap
  private static final ReentrantLock lock = new ReentrantLock();
  private static final boolean verboseLogging = false;

  private static class InFlightElement<OutputT> {
    final @Nullable Object key;
    final CompletableFuture<List<OutputT>> future;

    InFlightElement(@Nullable Object key, CompletableFuture<List<OutputT>> future) {
      this.key = key;
      this.future = future;
    }
  }

  // The In-Memory Accumulating Receiver
  // Accumulates elements in-memory during asynchronous background worker execution.
  // Buffered elements are only committed downstream once the parent task completes successfully
  // and the timer fires.
  private static class AccumulatingOutputReceiver<T> implements OutputReceiver<T> {
    private final List<T> outputs = new ArrayList<>();

    AccumulatingOutputReceiver() {}

    @Override
    public org.apache.beam.sdk.values.OutputBuilder<T> builder(T value) {
      return org.apache.beam.sdk.values.WindowedValues.<T>builder()
          .setValue(value)
          .setReceiver(windowedValue -> outputs.add(windowedValue.getValue()));
    }

    // Bypasses the nested anonymous OutputBuilder instantiation for standard outputs.
    // JVM optimization to prevent garbage collection pressure under high pipeline throughput.
    @Override
    public void output(T output) {
      outputs.add(output);
    }

    @Override
    public void outputWithTimestamp(T output, Instant timestamp) {
      outputs.add(output);
    }

    public List<T> getOutputs() {
      return outputs;
    }
  }

  public AsyncWrapper(
      DoFn<InputT, OutputT> syncFn,
      int parallelism,
      Duration timerFrequency,
      @Nullable Integer maxItemsToBuffer,
      @Nullable Duration timeout,
      @Nullable Duration maxWaitTime,
      @Nullable SerializableFunction<InputT, Object> idFn,
      boolean useThreadPool) {
    this(
        syncFn,
        parallelism,
        timerFrequency,
        maxItemsToBuffer,
        timeout,
        maxWaitTime,
        idFn,
        useThreadPool,
        null);
  }

  public AsyncWrapper(
      DoFn<InputT, OutputT> syncFn,
      int parallelism,
      Duration timerFrequency,
      @Nullable Integer maxItemsToBuffer,
      @Nullable Duration timeout,
      @Nullable Duration maxWaitTime,
      @Nullable SerializableFunction<InputT, Object> idFn,
      boolean useThreadPool,
      @Nullable Coder<KV<K, InputT>> coder) {
    this.syncFn = syncFn;
    this.parallelism = parallelism;
    if (timerFrequency.getMillis() <= 0) {
      throw new IllegalArgumentException("timerFrequency must be greater than zero");
    }
    this.timerFrequency = timerFrequency;
    this.maxItemsToBuffer =
        (maxItemsToBuffer != null)
            ? maxItemsToBuffer
            : Math.max(parallelism * 2, DEFAULT_MIN_BUFFER_CAPACITY);
    this.timeout = (timeout != null) ? timeout : Duration.standardSeconds(DEFAULT_TIMEOUT_SEC);
    this.maxWaitTime =
        (maxWaitTime != null) ? maxWaitTime : Duration.millis(DEFAULT_MAX_WAIT_TIME_MS);
    this.idFn =
        (idFn != null)
            ? idFn
            : (SerializableFunction<InputT, Object>)
                input -> java.util.Objects.requireNonNull(input);
    this.useThreadPool = useThreadPool;
    this.uuid = UUID.randomUUID().toString();
    this.toProcessSpec = (coder != null) ? StateSpecs.bag(coder) : StateSpecs.bag();
  }

  private ExecutorService getThreadPool() {
    ExecutorService threadPool = pool.get(uuid);
    if (threadPool == null) {
      throw new IllegalStateException("Thread pool not initialized for UUID: " + uuid);
    }
    return threadPool;
  }

  @SuppressWarnings("unchecked")
  private ConcurrentHashMap<Object, InFlightElement<OutputT>> getProcessingElements() {
    ConcurrentHashMap<Object, InFlightElement<?>> elements = processingElements.get(uuid);
    if (elements == null) {
      throw new IllegalStateException("Processing elements map not initialized for UUID: " + uuid);
    }
    return (ConcurrentHashMap<Object, InFlightElement<OutputT>>) (ConcurrentHashMap<?, ?>) elements;
  }

  private AtomicInteger getItemsInBuffer() {
    AtomicInteger buffer = itemsInBuffer.get(uuid);
    if (buffer == null) {
      throw new IllegalStateException("Buffer counter not initialized for UUID: " + uuid);
    }
    return buffer;
  }

  // Setup is called by the runner exactly once on each worker node when this DoFn is initialized.
  // It is responsible for setting up the wrapped synchronous DoFn
  // and initializing the shared JVM-wide thread pool and registries.
  @Setup
  public void setup(PipelineOptions options) {
    this.pipelineOptions = options;

    // Setup the wrapped DoFn
    DoFnInvokers.invokerFor(syncFn)
        .invokeSetup(
            new DoFnInvoker.BaseArgumentProvider<InputT, OutputT>() {
              @Override
              public PipelineOptions pipelineOptions() {
                return options;
              }

              @Override
              public String getErrorContext() {
                return "AsyncWrapper/Setup";
              }
            });

    if (useThreadPool) {
      LOG.info("Using thread pool for asynchronous execution with parallelism {}", parallelism);
    }

    lock.lock();
    try {
      if (useThreadPool) {
        pool.computeIfAbsent(uuid, k -> Executors.newFixedThreadPool(parallelism));
      }
      processingElements.computeIfAbsent(uuid, k -> new ConcurrentHashMap<>());
      itemsInBuffer.computeIfAbsent(uuid, k -> new AtomicInteger(0));
      refCounts.computeIfAbsent(uuid, k -> new AtomicInteger(0)).incrementAndGet();
    } finally {
      lock.unlock();
    }
  }

  // Clean up JVM-wide shared resources to prevent thread leaks on the worker
  @Teardown
  public void teardown() {
    DoFnInvokers.invokerFor(syncFn).invokeTeardown();
    ExecutorService threadPool = null;
    lock.lock();
    try {
      AtomicInteger refCount = refCounts.get(uuid);
      if (refCount != null && refCount.decrementAndGet() == 0) {
        refCounts.remove(uuid);
        threadPool = pool.remove(uuid);
        processingElements.remove(uuid);
        itemsInBuffer.remove(uuid);
      }
    } finally {
      lock.unlock();
    }
    if (threadPool != null) {
      threadPool.shutdown();
      try {
        if (!threadPool.awaitTermination(TEARDOWN_AWAIT_SEC, TimeUnit.SECONDS)) {
          threadPool.shutdownNow();
        }
      } catch (InterruptedException e) {
        threadPool.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  // Asynchronous Scheduling & Deduplication
  // Submits tasks to the background thread pool. If an element with the same ID is already
  // in-flight,
  // the submission is silently ignored to enforce exactly-once semantics.
  private boolean scheduleIfRoom(
      KV<K, InputT> element, BoundedWindow window, Instant timestamp, boolean ignoreBuffer) {
    lock.lock();
    try {
      ConcurrentHashMap<Object, InFlightElement<OutputT>> activeElements = getProcessingElements();
      Object elementId = idFn.apply(element.getValue());

      if (activeElements.containsKey(elementId)) {
        logInfo("Item " + element + " already in processing elements");
        return true;
      }

      int currentBuffer = getItemsInBuffer().get();
      if (currentBuffer < maxItemsToBuffer || ignoreBuffer) {
        java.util.concurrent.Executor executor =
            useThreadPool ? getThreadPool() : java.util.concurrent.ForkJoinPool.commonPool();

        // Pending asynchronous task that will produce a list of outputs
        CompletableFuture<List<OutputT>> future =
            CompletableFuture.supplyAsync(
                () -> {
                  try {
                    AccumulatingOutputReceiver<OutputT> receiver =
                        new AccumulatingOutputReceiver<>();
                    DoFnInvoker<InputT, OutputT> invoker = DoFnInvokers.invokerFor(syncFn);

                    DoFnInvoker.ArgumentProvider<InputT, OutputT> bundleArgProvider =
                        bundleArgProvider(receiver);
                    DoFnInvoker.ArgumentProvider<InputT, OutputT> processArgProvider =
                        processArgProvider(element, window, timestamp, receiver);

                    invoker.invokeStartBundle(bundleArgProvider);
                    invoker.invokeProcessElement(processArgProvider);
                    invoker.invokeFinishBundle(bundleArgProvider);

                    return receiver.getOutputs();
                  } catch (Exception e) {
                    throw new CompletionException(e);
                  }
                },
                executor);

        // Assigned to 'unused' to satisfy ErrorProne while preserving parent future for
        // cancellation
        CompletableFuture<List<OutputT>> unused =
            future.whenComplete(
                (res, ex) -> {
                  getItemsInBuffer().decrementAndGet();
                });

        // Add element to active elements map and increment buffer counter
        activeElements.put(elementId, new InFlightElement<>(element.getKey(), future));
        getItemsInBuffer().incrementAndGet();
        return true;
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  private DoFnInvoker.ArgumentProvider<InputT, OutputT> bundleArgProvider(
      AccumulatingOutputReceiver<OutputT> receiver) {
    return new BundleArgProvider(receiver);
  }

  private DoFnInvoker.ArgumentProvider<InputT, OutputT> processArgProvider(
      KV<K, InputT> element,
      BoundedWindow window,
      Instant timestamp,
      OutputReceiver<OutputT> receiver) {
    return new ProcessArgProvider(element, window, timestamp, receiver);
  }

  // Named BaseArgumentProvider supplying bundle-level lifecycle context to the invoker.
  private class BundleArgProvider extends DoFnInvoker.BaseArgumentProvider<InputT, OutputT> {
    private final AccumulatingOutputReceiver<OutputT> receiver;

    BundleArgProvider(AccumulatingOutputReceiver<OutputT> receiver) {
      this.receiver = receiver;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      PipelineOptions options = pipelineOptions;
      if (options == null) {
        throw new IllegalStateException("PipelineOptions not set");
      }
      return options;
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      return new AsyncFinishBundleContext(doFn, receiver);
    }

    @Override
    public String getErrorContext() {
      return "AsyncWrapper/Bundle";
    }
  }

  // FinishBundleContext subclass bound to the enclosing DoFn instance to prevent compiler crashes.
  private class AsyncFinishBundleContext extends DoFn<InputT, OutputT>.FinishBundleContext {
    private final AccumulatingOutputReceiver<OutputT> receiver;

    AsyncFinishBundleContext(
        DoFn<InputT, OutputT> doFn, AccumulatingOutputReceiver<OutputT> receiver) {
      doFn.super();
      this.receiver = receiver;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      PipelineOptions options = pipelineOptions;
      if (options == null) {
        throw new IllegalStateException("PipelineOptions not set");
      }
      return options;
    }

    @Override
    public void output(OutputT output, Instant timestamp, BoundedWindow window) {
      receiver.outputWithTimestamp(output, timestamp);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
      throw new UnsupportedOperationException(
          "Tagged output not supported in FinishBundleContext for AsyncWrapper");
    }
  }

  // BaseArgumentProvider supplying element-level context to the invoker.
  private class ProcessArgProvider extends DoFnInvoker.BaseArgumentProvider<InputT, OutputT> {
    private final KV<K, InputT> element;
    private final BoundedWindow window;
    private final Instant timestamp;
    private final OutputReceiver<OutputT> receiver;

    ProcessArgProvider(
        KV<K, InputT> element,
        BoundedWindow window,
        Instant timestamp,
        OutputReceiver<OutputT> receiver) {
      this.element = element;
      this.window = window;
      this.timestamp = timestamp;
      this.receiver = receiver;
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      return element.getValue();
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return receiver;
    }

    @Override
    public BoundedWindow window() {
      return window;
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return timestamp;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      PipelineOptions options = pipelineOptions;
      if (options == null) {
        throw new IllegalStateException("PipelineOptions not set");
      }
      return options;
    }

    @Override
    public String getErrorContext() {
      return "AsyncWrapper/Process";
    }
  }

  // Schedule an element to the thread pool, retries with backoff if the buffer is full.
  private void scheduleItem(KV<K, InputT> element, BoundedWindow window, Instant timestamp) {
    boolean done = false;
    long sleepTime = INITIAL_BACKOFF_SLEEP_MS;
    long totalSleep = 0;
    long timeoutMs = timeout.getMillis();

    while (!done && totalSleep < timeoutMs) {
      done = scheduleIfRoom(element, window, timestamp, false);
      if (!done) {
        long sleep = Math.min(maxWaitTime.getMillis(), sleepTime);
        logBackpressure(element, sleep, totalSleep);
        try {
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting for space in buffer", e);
        }

        // Prevents long overflow possibility
        if (sleepTime < maxWaitTime.getMillis()) {
          sleepTime *= 2;
        }

        totalSleep += sleep;
      }
    }
    // Timeout: element skips JVM pool but stays in BagState for timer to reschedule later.
  }

  // Uses hashcode based jitter instead of random for deterministic rescheduling
  // Satisfies lint check
  private Instant nextTimeToFire(@Nullable K key) {
    long seed = (key == null) ? 0 : key.hashCode();
    double fractionalOffset = Math.abs(seed % (long) HASH_MODULO_LIMIT) / HASH_MODULO_LIMIT;
    double timerFrequencySec = timerFrequency.getMillis() / MS_PER_SEC;
    double nowSec = System.currentTimeMillis() / MS_PER_SEC;

    double base = Math.floor((nowSec + timerFrequencySec) / timerFrequencySec) * timerFrequencySec;
    double offset = fractionalOffset * timerFrequencySec;

    return Instant.ofEpochMilli((long) ((base + offset) * MS_PER_SEC));
  }

  @ProcessElement
  public void processElement(
      ProcessContext c,
      BoundedWindow window,
      @StateId("to_process") BagState<KV<K, InputT>> toProcessState,
      @TimerId("timer") Timer timer) {

    KV<K, InputT> element = c.element();
    scheduleItem(element, window, c.timestamp());
    toProcessState.add(element);

    Instant timeToFire = nextTimeToFire(element.getKey());
    timer.set(timeToFire);
  }

  @OnTimer("timer")
  public void onTimer(
      OnTimerContext c,
      @StateId("to_process") BagState<KV<K, InputT>> toProcessState,
      @TimerId("timer") Timer timer,
      OutputReceiver<OutputT> receiver) {

    commitFinishedItems(c.fireTimestamp(), toProcessState, timer, receiver);
  }

  // Synchronizes local task results with the runner's persistent state container.
  // Emits successfully completed elements, cancels rolled-back tasks, and reschedules lost work.
  void commitFinishedItems(
      Instant fireTimestamp,
      BagState<KV<K, InputT>> toProcessState,
      Timer timer,
      OutputReceiver<OutputT> receiver) {

    Iterable<KV<K, InputT>> toProcessLocal = toProcessState.read();
    if (toProcessLocal == null || !toProcessLocal.iterator().hasNext()) {
      // Early Exit: if BagState is empty, we skip checking activeElements for this key.
      return;
    }

    // Since fireTimestamp is key-scoped, we determine the current key from the first element in
    // state
    K key = null;
    List<KV<K, InputT>> stateList = new ArrayList<>();
    for (KV<K, InputT> element : toProcessLocal) {
      stateList.add(element);
      if (key == null) {
        key = element.getKey();
      }
    }

    logInfo("processing timer for key: " + key);

    ConcurrentHashMap<Object, InFlightElement<OutputT>> activeElements = getProcessingElements();
    List<List<OutputT>> toReturn = new ArrayList<>();
    List<KV<K, InputT>> toReschedule = new ArrayList<>();

    int itemsFinished = 0;
    int itemsNotYetFinished = 0;
    int itemsRescheduled = 0;

    Set<Object> finishedElementIds = new HashSet<>();
    Set<Object> inFlightElementIds = new HashSet<>();
    Set<Object> rescheduledElementIds = new HashSet<>();

    lock.lock();
    try {
      Set<Object> stateElementIds = new HashSet<>();
      for (KV<K, InputT> element : stateList) {
        stateElementIds.add(idFn.apply(element.getValue()));
      }

      List<Object> toCancelIds = new ArrayList<>();
      for (Map.Entry<Object, InFlightElement<OutputT>> entry : activeElements.entrySet()) {
        InFlightElement<OutputT> inFlight = entry.getValue();
        if (java.util.Objects.equals(inFlight.key, key)
            && !stateElementIds.contains(entry.getKey())) {
          toCancelIds.add(entry.getKey());
        }
      }

      for (Object cancelId : toCancelIds) {
        InFlightElement<OutputT> inFlight = activeElements.get(cancelId);
        if (inFlight != null) {
          inFlight.future.cancel(true);
          activeElements.remove(cancelId);
        }
      }

      for (KV<K, InputT> element : stateList) {
        Object elementId = idFn.apply(element.getValue());

        // Skip processing if we already completed, rescheduled, or found this elementId active in
        // this cycle
        if (finishedElementIds.contains(elementId)
            || rescheduledElementIds.contains(elementId)
            || inFlightElementIds.contains(elementId)) {
          continue;
        }

        if (activeElements.containsKey(elementId)) {
          InFlightElement<OutputT> inFlight = activeElements.get(elementId);
          if (inFlight.future.isDone()) {
            try {
              if (!inFlight.future.isCancelled()) {
                toReturn.add(inFlight.future.get());
              }

              finishedElementIds.add(elementId);
              activeElements.remove(elementId);
              itemsFinished++;
            } catch (Exception e) {
              LOG.error("Error executing async task for element {}", element, e);
              throw new RuntimeException("Error executing async task for element " + element, e);
            }
          } else {
            inFlightElementIds.add(elementId);
            itemsNotYetFinished++;
          }
        } else {
          logInfo(
              "Item "
                  + element
                  + " found in state but not in local active elements, scheduling now");
          toReschedule.add(element);
          rescheduledElementIds.add(elementId);
          itemsRescheduled++;
        }
      }
    } finally {
      lock.unlock();
    }

    // Reschedule missing elements
    for (KV<K, InputT> element : toReschedule) {
      scheduleItem(element, GlobalWindow.INSTANCE, fireTimestamp);
    }

    // Update State: keep only unfinished items
    toProcessState.clear();
    int itemsInProcessingState = 0;
    for (KV<K, InputT> element : stateList) {
      Object elementId = idFn.apply(element.getValue());
      if (!finishedElementIds.contains(elementId)) {
        toProcessState.add(element);
        itemsInProcessingState++;
      }
    }

    // Emit completed outputs
    // (Emit completed tasks immediately; do not wait for all active tasks to finish).
    // Outputs use processing-time timestamps matching Python behavior
    for (List<OutputT> outputs : toReturn) {
      for (OutputT out : outputs) {
        receiver.output(out);
      }
    }

    logInfo(
        String.format(
            "Items finished: %d, not yet finished: %d, rescheduled: %d, in processing state: %d",
            itemsFinished, itemsNotYetFinished, itemsRescheduled, itemsInProcessingState));

    if (itemsInProcessingState > 0) {
      Instant timeToFire = nextTimeToFire(key);
      timer.set(timeToFire);
    }
  }

  private void logInfo(String s) {
    if (verboseLogging) {
      LOG.info("{}", s);
    }
  }

  private void logBackpressure(KV<K, InputT> element, long sleep, long totalSleep) {
    if (verboseLogging || totalSleep > BACKPRESSURE_LOG_THRESHOLD_MS) {
      LOG.info(
          "buffer is full for item {}, {} waiting {} ms. Have waited for {} ms.",
          element,
          getItemsInBuffer().get(),
          sleep,
          totalSleep);
    }
  }

  // Package-private helper methods for testing direct execution without Pipeline / ProcessContext
  // boilerplate
  @VisibleForTesting
  void processDirect(
      KV<K, InputT> element,
      BoundedWindow window,
      Instant timestamp,
      BagState<KV<K, InputT>> toProcessState,
      Timer timer) {
    scheduleItem(element, window, timestamp);
    toProcessState.add(element);
    Instant timeToFire = nextTimeToFire(element.getKey());
    timer.set(timeToFire);
  }

  @VisibleForTesting
  List<OutputT> commitFinishedItemsDirect(
      Instant fireTimestamp, BagState<KV<K, InputT>> toProcessState, Timer timer) {
    AccumulatingOutputReceiver<OutputT> receiver = new AccumulatingOutputReceiver<>();
    commitFinishedItems(fireTimestamp, toProcessState, timer, receiver);
    return receiver.getOutputs();
  }

  @VisibleForTesting
  boolean isEmpty() {
    return getItemsInBuffer().get() == 0;
  }

  @VisibleForTesting
  int getItemsInBufferCount() {
    return getItemsInBuffer().get();
  }

  @VisibleForTesting
  static void resetState() {
    lock.lock();
    try {
      for (Map.Entry<String, ExecutorService> entry : pool.entrySet()) {
        entry.getValue().shutdownNow();
      }
      pool.clear();
      processingElements.clear();
      itemsInBuffer.clear();
      refCounts.clear();
    } finally {
      lock.unlock();
    }
  }
}
