package org.apache.beam.sdk.transforms;

import com.google.auto.value.AutoValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.util.MemoizingPerInstantiationSerializableSupplier;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
   * An abstract {@link DoFn} that allows processing elements asynchronously.
   *
   * <p>This {@link DoFn} provides a framework for managing asynchronous operations, ensuring that
   * elements are processed in a non-blocking manner while maintaining proper synchronization and
   * handling of results.
   *
   * <p>To use this, extend {@link org.apache.beam.sdk.transforms.AsyncDoFn} and implement the {@link #asyncProcessElement} method to
   * initiate the asynchronous operation.
   *
   * @param <K> The key of KV of input elements
   * @param <InputT> The value of the KV of the input elements.
   * @param <OutputT> The type of the output elements.
   */
  public abstract class AsyncDoFn<K,InputT,OutputT> extends DoFn<KV<K,InputT>, OutputT> {

    private static final Logger LOG = LoggerFactory.getLogger(org.apache.beam.sdk.transforms.AsyncDoFn.class);

    /**
     * A {@link TimerSpec} object used for managing asynchronous callbacks or recovering from
     * lost in-memory state.
     */
    @TimerId("timer")
    private final TimerSpec pollTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    /**
     * A {@link StateSpec} object used for storing elements that are waiting to be processed.
     */
    @StateId("buffer")
    private final StateSpec<BagState<InputT>> buffer =  StateSpecs.bag();

    static class LocalState<K, InputT, OutputT> {
      LocalState(ExecutorService executorService, int maxItemsToBuffer) {
        this.executorService = executorService;
        this.bufferedElementsSemaphore = new Semaphore(maxItemsToBuffer);
      }
      ExecutorService executorService;
      Semaphore bufferedElementsSemaphore;
      static class ProcessingElements<InputT, OutputT> {
        ProcessingElements(InputT element, CompletableFuture<OutputT> future) {
          this.element = element;
          this.future = future;
        }
        final InputT element;
        final CompletableFuture<OutputT> future;
      }
      ConcurrentHashMap<Object, ProcessingElements<InputT, OutputT>> processingElements;
    }
    private final MemoizingPerInstantiationSerializableSupplier<LocalState<K, InputT, OutputT>>
      localState;

    /**
     *  Configurable parameters for the AsyncDoFn.
     */
    private final int callbackFrequencyMillis;
    private final int maxItemsToBuffer;
    private final int timeoutMillis;
    private final int maxWaitTimeMillis;
    private final SerializableFunction<InputT, Object> idFn;

    protected AsyncDoFn(AsyncDoFnOptions<InputT> options) {
      this.callbackFrequencyMillis = options.getCallbackFrequencyMillis();
      this.maxItemsToBuffer = options.getMaxItemsToBuffer();
      this.timeoutMillis = options.getTimeoutMillis();
      this.maxWaitTimeMillis = options.getMaxWaitTimeMillis();
      this.idFn = options.getIdFn();
      final int parallelism = options.getParallelism();
      this.localState = new MemoizingPerInstantiationSerializableSupplier<>(
         () -> {
          ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
          executor.setMaximumPoolSize(parallelism);
          return new LocalState<>(executor, maxItemsToBuffer);
        });
    }

    /**
     * Processes an input element asynchronously.
     *
     * <p>This method should initiate the asynchronous operation and return immediately. The result
     * of the operation should be completed by calling the {@link #onResult} method.
     *
     * @param c The process context.
     */
    @ProcessElement
    public void processElement(
      ProcessContext c,
      @TimerId("timer") Timer timer,
      @StateId("buffer") BagState<KV<K, InputT>> toProcess) {

      scheduleItem(c.element(), c.timestamp(), timer, toProcess, c);

      // Don't output any elements. This will be done in commitFinishedItems.
    }

    private void scheduleItem(
      KV<K, InputT> element,
      Instant timestamp,
      Timer timer,
      BagState<KV<K, InputT>> toProcess,
      ProcessContext c) {
      LocalState<K, InputT, OutputT> local = localState.get();

      Object elementId = idFn.apply(element.getValue());
      try {
        if (local.bufferedElementsSemaphore.tryAcquire(maxWaitTimeMillis, TimeUnit.MILLISECONDS)) {
          CompletableFuture<OutputT> future = new CompletableFuture<>();
          local.processingElements.put(elementId,
            new LocalState.ProcessingElements<>(element.getValue(), future));
          local.executorService.execute(() -> {
            try {
              asyncProcessElement(element.getValue(), future);
            } catch (Exception e) {
              future.completeExceptionally(e);
            }
          });
        } else {
          LOG.warn("Unable to schedule due max buffered limit {}, buffering on disk and will schedule with timers.",
                   maxItemsToBuffer);
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }

      toProcess.add(element);
      // XXX what about the watermark?
      timer.set(nextTimeToFire(element.getKey()));
    }

    /**
     * Override this method to implement the asynchronous processing logic. Initiate the
     * asynchronous operation, and when the result is available, complete the future with the
     * result.
     *
     * Note that this may run
     *
     * @param element The input element.
     * @param future The future to complete with the result of the asynchronous operation.
     */
    protected abstract void asyncProcessElement(InputT element, CompletableFuture<OutputT> future) throws Exception;

    private Instant nextTimeToFire(K key) {
      long base = Math.floorDiv(System.currentTimeMillis() + callbackFrequencyMillis, callbackFrequencyMillis);
      long offset = Math.floorMod(Math.abs(key.hashCode()), callbackFrequencyMillis);
      return Instant.ofEpochMilli(base + offset);
    }

    /**
     * Commits finished items and synchronizes local state with runner state.
     *
     * <p>Note timer firings are per key while local state contains messages for all keys. Only
     * messages for the given key will be output/cleaned up.
     *
     * @param toProcess State that keeps track of queued messages for this key.
     * @param timer Timer that initiated this commit and can be reset if not all items have finished..
     * @return A list of elements that have finished processing for this key.
     */
    @OnTimer("timer")
    public void timerCallback(
      OnTimerContext c,
      @AlwaysFetched @StateId("buffer") BagState<KV<K, InputT>> toProcess,
      @TimerId("timer") Timer timer) throws Exception {
      commitFinishedItems(c, toProcess, timer);
    }

    private void commitFinishedItems(
      OnTimerContext c,
      BagState<KV<K, InputT>> toProcess,
      Timer timer) throws Exception {
      // For all elements that are in processing state:
      // If the element is done processing, delete it from all state and yield the output.
      // If the element is not yet done, print it. If the element is not in local state, schedule it for processing.

      HashMap<Object, InputT> bufferMap = new HashMap<>();
      @Nullable K key = null;
      long toProcessCount = 0;
      for (KV<K, InputT> elem : toProcess.read()) {
        ++toProcessCount;
        if (toProcessCount == 1) {
          key = elem.getKey();
        }
        Object id = idFn.apply(elem.getValue());
        InputT evicted = bufferMap.put(id, elem.getValue());
        if  (evicted != null) {
          if (evicted.equals(elem)) {
            LOG.error("Unexpected duplicate elements in buffer, only a single processing and output will be made.");
          } else {
            LOG.error("Unexpected id equality with differing elements! This is resulting in lost data. id={}, elem1={}, elem2={}",
              id, elem, evicted);
          }
        }
      }
      if (toProcessCount == 0) {
        // No buffered elements.
        return;
      }

      LOG.debug("processing timer for key: {}", key);

      LocalState<K, InputT, OutputT> state = localState.get();

      final List<OutputT> outputs = new ArrayList<>();
      final List<Object> finishedIds = new ArrayList<>();
      final List<InputT> itemsToReschedule = new ArrayList<>();


      for (final Iterator<Map.Entry<Object, InputT>> i = bufferMap.entrySet().iterator();
           i.hasNext();) {
        final Map.Entry<Object, InputT> elem = i.next();
        final Object id = elem.getKey();
        final InputT element = elem.getValue();

        state.processingElements.compute(
          id,
          (@Nullable Object ignored,
           @Nullable LocalState.ProcessingElements<InputT, OutputT> existing) -> {
            if (existing == null) {
              LOG.info(
                "item {} found in processing state but not local state, scheduling now",
                element);
              itemsToReschedule.add(element);
              return null;
            }

            if (!existing.future.isDone()) {
              return existing;
            }
            try {
              outputs.add(existing.future.get());
              i.remove();
            } catch (ExecutionException | InterruptedException e) {
              LOG.error("Error processing asynchronously, retrying", e);
              itemsToReschedule.add(element);
            }
            state.bufferedElementsSemaphore.release();
            return null;
          });
      }
      for (OutputT o : outputs) {
        c.output(o);
      }

      // Update processing state to remove elements we've finished
        if (bufferMap.size() != toProcessCount) {
          toProcess.clear();
          final K finalKey = key;
          bufferMap.forEach((k, v) -> toProcess.add(KV.of(finalKey, v)));
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("items finished {}, items rescheduling {}, items in buffer {}",
            outputs.size(), itemsToReschedule.size(),
            maxItemsToBuffer - state.bufferedElementsSemaphore.availablePermits());
        }

        if (!bufferMap.isEmpty()) {
          timer.set(nextTimeToFire(key));
        }
      }
    }

    /**
     * Configurable parameters for the AsyncDoFn.
     */
    @AutoValue
    public abstract static class AsyncDoFnOptions<InputT> {

      /**
       *  The maximum number of elements to process in parallel per worker for this dofn.
       */
      public abstract int getParallelism();

      /**
       *  The frequency with which the runner will check for elements to commit.
       */
      public abstract int getCallbackFrequencyMillis();

      /**
       *  We should ideally buffer enough to always be busy but not so much that the worker ooms.
       */
      public abstract int getMaxItemsToBuffer();

      /**
       *  The maximum amount of time an item should try to be scheduled locally before it goes in the queue of waiting work.
       */
      public abstract int getTimeoutMillis();

      /**
       *  The maximum amount of sleep time while attempting to schedule an item.
       */
      public abstract int getMaxWaitTimeMillis();

      /**
       * A function that extracts an id from an element to be used as keys in maps.
       * The id should have a 1:1 relationship with the entire element, that is
       * getIdFn()(input1).equals(getIdFn()(input2)) iff input1.equals(input2).
       * By default, the id is the entire element.
       */
      public abstract SerializableFunction<InputT, Object> getIdFn();

      public static <InputT> Builder<InputT> builder() {
        return new AutoValue_AsyncDoFn_AsyncDoFnOptions.Builder<InputT>()
          .setParallelism(1)
          .setCallbackFrequencyMillis(5)
          .setMaxItemsToBuffer(20)
          .setTimeoutMillis(1)
          .setMaxWaitTimeMillis(1)
          .setIdFn(i -> i);
      }

      @AutoValue.Builder
      public abstract static class Builder<InputT> {
        public abstract Builder<InputT> setParallelism(int parallelism);

        public abstract Builder<InputT> setCallbackFrequencyMillis(int callbackFrequencyMillis);

        public abstract Builder<InputT> setMaxItemsToBuffer(int maxItemsToBuffer);

        public abstract Builder<InputT> setTimeoutMillis(int timeoutMillis);

        public abstract Builder<InputT> setMaxWaitTimeMillis(double maxWaitTimeMillis);

        public abstract Builder<InputT> setIdFn(Function<InputT, Object> idFn);

        public abstract AsyncDoFnOptions build();
      }
    }
  }
