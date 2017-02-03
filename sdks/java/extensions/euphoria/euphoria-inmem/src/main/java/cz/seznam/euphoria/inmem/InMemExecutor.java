/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.inmem;

import com.google.common.collect.Iterables;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.core.executor.FlowUnfolder.InputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Inmem executor for testing.
 */
public class InMemExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(InMemExecutor.class);

  @FunctionalInterface
  private interface Supplier {
    Datum get() throws InterruptedException;
  }

  static final class PartitionSupplierStream implements Supplier {

    final Reader<?> reader;
    final Partition<?> partition;
    PartitionSupplierStream(Partition<?> partition) {
      this.partition = partition;
      try {
        this.reader = partition.openReader();
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to open reader for partition: " + partition, e);
      }
    }

    @Override
    public Datum get() {
      if (!this.reader.hasNext()) {
        try {
          this.reader.close();
        } catch (IOException e) {
          throw new RuntimeException(
              "Failed to close reader for partition: " + this.partition, e);
        }
        return Datum.endOfStream();
      }
      Object next = this.reader.next();
      // we assign it to batch
      // which means null group, and batch label
      return Datum.of(Batch.BatchWindow.get(), next,
          // ingestion time
          System.currentTimeMillis());
    }
  }

  /** Partitioned provider of input data for single operator. */
  private static final class InputProvider implements Iterable<Supplier> {
    final List<Supplier> suppliers;
    InputProvider() {
      this.suppliers = new ArrayList<>();
    }

    public int size() {
      return suppliers.size();
    }

    public void add(Supplier s) {
      suppliers.add(s);
    }

    public Supplier get(int i) {
      return suppliers.get(i);
    }

    @Override
    public Iterator<Supplier> iterator() {
      return suppliers.iterator();
    }

    Stream<Supplier> stream() {
      return suppliers.stream();
    }
  }

  static class QueueCollector implements Collector<Datum> {
    static QueueCollector wrap(BlockingQueue<Datum> queue) {
      return new QueueCollector(queue);
    }
    private final BlockingQueue<Datum> queue;
    QueueCollector(BlockingQueue<Datum> queue) {
      this.queue = queue;
    }
    @Override
    public void collect(Datum elem) {
      try {
        queue.put(elem);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private static final class QueueSupplier implements Supplier {

    static QueueSupplier wrap(BlockingQueue<Datum> queue) {
      return new QueueSupplier(queue);
    }

    private final BlockingQueue<Datum> queue;

    QueueSupplier(BlockingQueue<Datum> queue) {
      this.queue = queue;
    }

    @Override
    public Datum get() throws InterruptedException {
      return queue.take();
    }
  }

  private static final class ExecutionContext {
    // map of operator inputs to suppliers
    Map<Pair<Operator<?, ?>, Operator<?, ?>>, InputProvider> materializedOutputs
        = Collections.synchronizedMap(new HashMap<>());
    // already running operators
    Set<Operator<?, ?>> runningOperators = Collections.synchronizedSet(
        new HashSet<>());

    private boolean containsKey(Pair<Operator<?, ?>, Operator<?, ?>> d) {
      return materializedOutputs.containsKey(d);
    }
    void add(Operator<?, ?> source, Operator<?, ?> target,
        InputProvider partitions) {
      Pair<Operator<?, ?>, Operator<?, ?>> edge = Pair.of(source, target);
      if (containsKey(edge)) {
        throw new IllegalArgumentException("Dataset for edge "
            + edge + " is already materialized!");
      }
      materializedOutputs.put(edge, partitions);
    }
    InputProvider get(Operator<?, ?> source, Operator<?, ?> target) {
      Pair<Operator<?, ?>, Operator<?, ?>> edge = Pair.of(source, target);
      InputProvider sup = materializedOutputs.get(edge);
      if (sup == null) {
        throw new IllegalArgumentException(String.format(
            "Do not have suppliers for edge %s -> %s (original producer %s )",
            source, target, source.output().getProducer()));
      }
      return sup;
    }
    void markRunning(Operator<?, ?> operator) {
      if (!this.runningOperators.add(operator)) {
        throw new IllegalStateException("Twice running the same operator?");
      }
    }
    boolean isRunning(Operator<?, ?> operator) {
      return runningOperators.contains(operator);
    }
  }

  private final BlockingQueue<Runnable> queue = new SynchronousQueue<>(false);
  private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
      0, Integer.MAX_VALUE,
      60, TimeUnit.SECONDS,
      queue,
      new ThreadFactory() {
        ThreadFactory factory = Executors.defaultThreadFactory();
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = factory.newThread(r);
          thread.setDaemon(true);
          thread.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
            e.printStackTrace(System.err);
          });
          return thread;
        }
      });

  private java.util.function.Supplier<WatermarkEmitStrategy> watermarkEmitStrategySupplier
      = WatermarkEmitStrategy.Default::new;
  private java.util.function.Supplier<TriggerScheduler> triggerSchedulerSupplier
      = ProcessingTimeTriggerScheduler::new;

  private boolean allowWindowBasedShuffling = false;

  private StorageProvider storageProvider = new InMemStorageProvider();

  /**
   * Set supplier for watermark emit strategy used in state operations.
   * Defaults to {@code WatermarkEmitStrategy.Default}.
   *
   * @param supplier the watermark supplier
   *
   * @return this instance (for method chaining purposes)
   *
   * @throws NullPointerException if the given reference is {@code null}
   */
  public InMemExecutor setWatermarkEmitStrategySupplier(
      java.util.function.Supplier<WatermarkEmitStrategy> supplier) {
    this.watermarkEmitStrategySupplier = Objects.requireNonNull(supplier);
    return this;
  }

  /**
   * Set supplier for {@code TriggerScheduler} to be used in state operations.
   * Default is {@code ProcessingTimeTriggerScheduler}.
   *
   * @param supplier the scheduler supplier
   *
   * @return this instance (for method chaining purposes)
   *
   * @throws NullPointerException if the given reference is {@code null}
   */
  public InMemExecutor setTriggeringSchedulerSupplier(
      java.util.function.Supplier<TriggerScheduler> supplier) {
    this.triggerSchedulerSupplier = Objects.requireNonNull(supplier);
    return this;
  }
  
  /**
   * Enable shuffling of windowed data based on the windowID.
   *
   * @return this instance (for method chaining purposes)
   */
  public InMemExecutor allowWindowBasedShuffling() {
    this.allowWindowBasedShuffling = true;
    return this;
  }

  /**
   * Set provider for state's storage. Defaults to {@code InMemStorageProvider}.
   *
   * @param provider the storage provide
   *
   * @return this instance (for method chaining purposes)
   *
   * @throws NullPointerException if the given reference is {@code null}
   */
  public InMemExecutor setStateStorageProvider(StorageProvider provider) {
    this.storageProvider = Objects.requireNonNull(provider);
    return this;
  }
  
  @Override
  public CompletableFuture<Executor.Result> submit(Flow flow) {
    return CompletableFuture.supplyAsync(() -> execute(flow), executor);
  }
  
  @Override
  public void shutdown() {
    LOG.info("Shutting down inmem executor.");
    executor.shutdownNow();
  }

  private Executor.Result execute(Flow flow) {
    // transform the given flow to DAG of basic operators
    DAG<Operator<?, ?>> dag = FlowUnfolder.unfold(flow, Executor.getBasicOps());

    final List<Future> runningTasks = new ArrayList<>();
    Collection<Node<Operator<?, ?>>> leafs = dag.getLeafs();

    List<ExecUnit> units = ExecUnit.split(dag);

    if (units.isEmpty()) {
      throw new IllegalArgumentException("Cannot execute empty flow");
    }

    for (ExecUnit unit : units) {
      ExecutionContext context = new ExecutionContext();

      execUnit(unit, context);
    
      runningTasks.addAll(consumeOutputs(unit.getLeafs(), context));
    }

    // extract all processed sinks
    List<DataSink<?>> sinks = leafs.stream()
            .map(n -> n.get().output().getOutputSink())
            .filter(s -> s != null)
            .collect(Collectors.toList());

    // wait for all threads to finish
    for (Future<?> f : runningTasks) {
      try {
        f.get();
      } catch (InterruptedException e) {
        break;
      } catch (ExecutionException e) {
        // when any one of the tasks fails rollback all sinks and fail
        for (DataSink<?> s : sinks) {
          try {
            s.rollback();
          } catch (Exception ex) {
            LOG.error("Exception during DataSink rollback", ex);
          }
        }
        throw new RuntimeException(e);
      }
    }

    // commit all sinks
    try {
      for (DataSink<?> s : sinks) {
        s.commit();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return new Executor.Result();
  }

  /** Read all outputs of given nodes and store them using their sinks. */
  @SuppressWarnings("unchecked")
  private List<Future<?>> consumeOutputs(
      Collection<Node<Operator<?, ?>>> leafs,
      ExecutionContext context) {
    
    List<Future<?>> tasks = new ArrayList<>();
    // consume outputs
    for (Node<Operator<?, ?>> output : leafs) {
      DataSink<?> sink = output.get().output().getOutputSink();
      sink.initialize();
      final InputProvider provider = context.get(output.get(), null);
      int part = 0;
      for (Supplier s : provider) {
        final Writer writer = sink.openWriter(part++);
        tasks.add(executor.submit(() -> {
          try {
            for (;;) {
              Datum datum = s.get();
              if (datum.isEndOfStream()) {
                // end of the stream
                writer.flush();
                writer.commit();
                writer.close();
                // and terminate the thread
                break;
              } else if (datum.isElement()) {
                // ~ unwrap the bare bone element
                writer.write(datum.getElement());
              }
            }
          } catch (IOException ex) {
            rollbackWriterUnchecked(writer);
            // propagate exception
            throw new RuntimeException(ex);
          } catch (InterruptedException ex) {
            rollbackWriterUnchecked(writer);
          } finally {
            try {
              writer.close();
            } catch (IOException ioex) {
              LOG.warn("Something went wrong", ioex);
              // swallow exception
            }
          }
        }));
      }
    }
    return tasks;
  }

  // rollback writer and do not throw checked exceptions
  private void rollbackWriterUnchecked(Writer writer) {
    try {
      writer.rollback();
    } catch (IOException ex) {
      LOG.error("Failed to rollback writer", ex);
    }
  }

  private InputProvider createStream(DataSource<?> source) {
    InputProvider ret = new InputProvider();

    source.getPartitions().stream()
        .map(PartitionSupplierStream::new)
        .forEach(ret::add);

    return ret;
  }

  private void execUnit(ExecUnit unit, ExecutionContext context) {
    unit.getDAG().traverse().forEach(n -> execNode(n, context));
  }

  /**
   * Execute single operator and return the suppliers for partitions
   * of output.
   */
  @SuppressWarnings("unchecked")
  private void execNode(
      Node<Operator<?, ?>> node, ExecutionContext context) {
    Operator<?, ?> op = node.get();
    final InputProvider output;
    if (context.isRunning(op)) {
      return;
    }
    if (op instanceof InputOperator) {
      output = createStream(op.output().getSource());
    } else if (op instanceof FlatMap) {
      output = execMap((Node) node, context);
    } else if (op instanceof Repartition) {
      output = execRepartition((Node) node, context);
    } else if (op instanceof ReduceStateByKey) {
      output = execReduceStateByKey((Node) node, context);
    } else if (op instanceof Union) {
      output = execUnion((Node) node, context);
    } else {
      throw new IllegalStateException("Invalid operator: " + op);
    }
    context.markRunning(op);
    // store output for each child
    if (node.getChildren().size() > 1) {
      List<List<BlockingQueue<?>>> forkedProviders = new ArrayList<>();
      for (Node<Operator<?, ?>> ch : node.getChildren()) {
        List<BlockingQueue<?>> forkedProviderQueue = new ArrayList<>();
        InputProvider forkedProvider = new InputProvider();
        forkedProviders.add(forkedProviderQueue);
        for (int p = 0; p < output.size(); p++) {
          BlockingQueue<Datum> queue = new ArrayBlockingQueue<>(5000);
          forkedProviderQueue.add(queue);
          forkedProvider.add((Supplier) QueueSupplier.wrap(queue));
        }
        context.add(node.get(), ch.get(), forkedProvider);
      }
      for (int p = 0; p < output.size(); p++) {
        int partId = p;
        Supplier partSup = output.get(p);
        List<BlockingQueue<?>> outputs = forkedProviders.stream()
            .map(l -> l.get(partId)).collect(Collectors.toList());
        executor.execute(() -> {
          // copy the original data to all queues
          for (;;) {
            try {
              Datum item = partSup.get();
              for (BlockingQueue ch : outputs) {
                try {
                  ch.put(item);
                } catch (InterruptedException ex) {
                  Thread.currentThread().interrupt();
                }
              }
              if (item.isEndOfStream()) {
                return;
              }
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        });

      }

    } else if (node.getChildren().size() == 1) {
      context.add(node.get(), node.getChildren().iterator().next().get(), output);
    } else {
      context.add(node.get(), null, output);
    }
  }

  @SuppressWarnings("unchecked")
  private InputProvider execMap(Node<FlatMap> flatMap,
      ExecutionContext context) {
    InputProvider suppliers = context.get(
        flatMap.getSingleParentOrNull().get(), flatMap.get());
    InputProvider ret = new InputProvider();
    final UnaryFunctor mapper = flatMap.get().getFunctor();
    for (Supplier s : suppliers) {
      final BlockingQueue<Datum> out = new ArrayBlockingQueue(5000);
      ret.add(QueueSupplier.wrap(out));
      executor.execute(() -> {
        Collector collector = QueueCollector.wrap(out);
        for (;;) {
          try {
            // read input
            Datum item = s.get();
            WindowedElementCollector outC = new WindowedElementCollector(
                collector, item::getTimestamp);
            if (item.isElement()) {
              // transform
              outC.setWindow(item.getWindow());
              mapper.apply(item.getElement(), outC);
            } else {
              out.put(item);
              if (item.isEndOfStream()) {
                break;
              }
            }
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      });
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  private InputProvider execRepartition(
      Node<Repartition> repartition,
      ExecutionContext context) {

    Partitioning partitioning = repartition.get().getPartitioning();
    int numPartitions = partitioning.getNumPartitions();
    InputProvider input = context.get(
        repartition.getSingleParentOrNull().get(), repartition.get());
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("Cannot repartition input to "
          + numPartitions + " partitions");
    }

    List<BlockingQueue<Datum>> outputQueues = repartitionSuppliers(
        input, e -> e, partitioning, Optional.empty(), Optional.empty());

    InputProvider ret = new InputProvider();
    outputQueues.stream()
        .map(QueueSupplier::new)
        .forEach(s -> ret.add((Supplier) s));
    return ret;
  }

  @SuppressWarnings("unchecked")
  private InputProvider execReduceStateByKey(
      Node<ReduceStateByKey> reduceStateByKeyNode,
      ExecutionContext context) {

    final ReduceStateByKey reduceStateByKey = reduceStateByKeyNode.get();
    final UnaryFunction keyExtractor = reduceStateByKey.getKeyExtractor();
    final UnaryFunction valueExtractor = reduceStateByKey.getValueExtractor();

    InputProvider suppliers = context.get(
        reduceStateByKeyNode.getSingleParentOrNull().get(),
        reduceStateByKeyNode.get());

    final Partitioning partitioning = reduceStateByKey.getPartitioning();
    final Windowing windowing = reduceStateByKey.getWindowing();
    final UnaryFunction eventTimeAssigner = reduceStateByKey.getEventTimeAssigner();

    List<BlockingQueue<Datum>> repartitioned = repartitionSuppliers(
        suppliers, keyExtractor, partitioning,
        Optional.ofNullable(windowing), Optional.ofNullable(eventTimeAssigner));

    InputProvider outputSuppliers = new InputProvider();
    TriggerScheduler triggerScheduler = triggerSchedulerSupplier.get();
    final long watermarkDuration
        = triggerScheduler instanceof WatermarkTriggerScheduler
            ? ((WatermarkTriggerScheduler) triggerScheduler).getWatermarkDuration()
            : 0L;
    // consume repartitioned suppliers
    int i = 0;
    for (BlockingQueue<Datum> q : repartitioned) {
      final BlockingQueue<Datum> output = new ArrayBlockingQueue<>(5000);
      outputSuppliers.add(QueueSupplier.wrap(output));
      executor.execute(new ReduceStateByKeyReducer(
          reduceStateByKey,
          reduceStateByKey.getName() + "#part-" + (i++),
          q, output, keyExtractor, valueExtractor,
          // ~ on batch input we use a noop trigger scheduler
          // ~ if using attached windowing, we have to use watermark triggering
          reduceStateByKey.input().isBounded()
            ? new NoopTriggerScheduler()
            : (windowing != null
                  ? triggerSchedulerSupplier.get()
                  : new WatermarkTriggerScheduler(watermarkDuration)),
          watermarkEmitStrategySupplier.get(),
          storageProvider));
    }
    return outputSuppliers;
  }

  @SuppressWarnings("unchecked")
  private List<BlockingQueue<Datum>> repartitionSuppliers(
      InputProvider suppliers,
      final UnaryFunction keyExtractor,
      final Partitioning partitioning,
      final Optional<Windowing> windowing,
      final Optional<UnaryFunction> eventTimeAssigner) {

    int numInputPartitions = suppliers.size();
    final boolean isMergingWindowing = windowing.isPresent()
        && windowing.get() instanceof MergingWindowing;
    final boolean hasTimeAssignment = eventTimeAssigner.isPresent();
    
    final int outputPartitions = partitioning.getNumPartitions() > 0
        ? partitioning.getNumPartitions() : numInputPartitions;
    final List<BlockingQueue<Datum>> ret = new ArrayList<>(outputPartitions);
    for (int i = 0; i < outputPartitions; i++) {
      ret.add(new ArrayBlockingQueue(5000));
    }

    // count running partition readers
    CountDownLatch workers = new CountDownLatch(numInputPartitions);
    // vector clocks associated with each output partition
    List<VectorClock> clocks = new ArrayList<>(outputPartitions);
    for (int i = 0; i < outputPartitions; i++) {
      // each vector clock has as many
      clocks.add(new VectorClock(numInputPartitions));
    }

    // if we have timestamp assigner we need watermark emit strategy
    // associated with each input partition - this strategy then emits
    // watermarks to downstream partitions based on elements flowing
    // through the partition
    WatermarkEmitStrategy[] emitStrategies = new WatermarkEmitStrategy[numInputPartitions];
    for (int i = 0; i < emitStrategies.length; i++) {
      emitStrategies[i] = watermarkEmitStrategySupplier.get();
    }

    int i = 0;
    for (Supplier s : suppliers) {
      final int readerId = i++;
      // each input partition maintains maximum element's timestamp that
      // passed through it
      final AtomicLong maxElementTimestamp = new AtomicLong();
      Runnable emitWatermark = () -> {
        handleMetaData(
            Datum.watermark(maxElementTimestamp.get()),
            ret, readerId, clocks, true);
      };
      if (emitStrategies != null) {
        emitStrategies[readerId].schedule(emitWatermark);
      }
      executor.execute(() -> {
        try {
          for (;;) {
            // read input
            Datum datum = s.get();
            
            if (datum.isEndOfStream()) {
              break;
            }

            if (eventTimeAssigner.isPresent() && datum.isElement()) {
              UnaryFunction assigner = eventTimeAssigner.get();
              datum.setTimestamp((long) assigner.apply(datum.getElement()));
            }

            if (!handleMetaData(datum, ret, readerId, clocks, !hasTimeAssignment)) {

              // extract element's timestamp if available
              long elementStamp = datum.getTimestamp();
              maxElementTimestamp.accumulateAndGet(elementStamp,
                  (oldVal, newVal) -> oldVal < newVal ? newVal : oldVal);
              // determine partition
              Object key = keyExtractor.apply(datum.getElement());
              final Set<Window> targetWindows;
              int windowShift = 0;
              if (allowWindowBasedShuffling) {
                if (windowing.isPresent()) {
                  // FIXME: the time function inside windowing
                  // must be part of the operator itself
                  targetWindows = windowing.get().assignWindowsToElement(datum);
                } else {
                  targetWindows = Collections.singleton(datum.getWindow());
                }

                if (!isMergingWindowing && targetWindows.size() == 1) {
                  windowShift = new Random(
                      Iterables.getOnlyElement(targetWindows).hashCode()).nextInt();
                }
              }
              int partition
                  = ((partitioning.getPartitioner().getPartition(key) + windowShift)
                      & Integer.MAX_VALUE) % outputPartitions;
              // write to the correct partition
              ret.get(partition).put(datum);
            }
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        } finally {
          workers.countDown();
        }
      });
    }
    waitForStreamEnds(workers, ret);
    return ret;
  }

  // wait until runningTasks is not zero and then send EOF to all output queues
  private void waitForStreamEnds(
      CountDownLatch fire, List<BlockingQueue<Datum>> outputQueues) {
    // start a new task that will wait for all read partitions to end
    executor.execute(() -> {
      try {
        fire.await();
      } catch (InterruptedException ex) {
        LOG.warn("waiting-for-stream-ends interrupted");
        Thread.currentThread().interrupt();
      }
      // try sending eof to all outputs
      for (BlockingQueue<Datum> queue : outputQueues) {
        try {
          queue.put(Datum.endOfStream());
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
    });
  }

  @SuppressWarnings("unchecked")
  private InputProvider execUnion(
      Node<Union> union, ExecutionContext context) {

    InputProvider ret = new InputProvider();
    union.getParents().stream()
        .flatMap(p -> context.get(p.get(), union.get()).stream())
        .forEach(s -> ret.add((Supplier) s));
    return ret;
  }

  /**
   * Abort execution of all tasks.
   */
  public void abort() {
    executor.shutdownNow();
  }

  // utility method used after extracting element from upstream
  // queue, the element is passed to the downstream partitions
  // if it is a metadata element
  // @returns true if the element was handled
  static boolean handleMetaData(
      Datum item,
      List<BlockingQueue<Datum>> downstream,
      int readerId,
      List<VectorClock> clocks,
      boolean allowWatermarks) {


    if (allowWatermarks || !item.isWatermark()) {
      // update vector clocks by the watermark
      // if any update occurs, emit the updates time downstream
      int i = 0;
      long stamp = item.getTimestamp();
      for (VectorClock clock : clocks) {
        long current = clock.getCurrent();
        clock.update(stamp, readerId);
        long after = clock.getCurrent();
        if (current != after) {
          try {
            // we have updated the stamp, emit the new watermark
            downstream.get(i).put(Datum.watermark(after));
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
        i++;
      }
    }

    // watermark already handled
    if (item.isWatermark()) return true;

    // do not hadle elements
    if (item.isElement()) return false;
    
    // propagate window triggers to downstream consumers
    if (item.isWindowTrigger()) {
      try {
        for (BlockingQueue<Datum> ch : downstream) {
          ch.put(item);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // was handled
    return true;
  }
}
