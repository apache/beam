
package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.Triggering;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.Collector;
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
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.ExecUnit;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.core.executor.FlowUnfolder.InputOperator;
import cz.seznam.euphoria.core.executor.SerializableUtils;
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
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Inmem executor for testing.
 */
public class InMemExecutor implements Executor {

  private static final Logger LOG = LoggerFactory.getLogger(InMemExecutor.class);

  @FunctionalInterface
  private interface Supplier<T> {
    T get() throws EndOfStreamException;
  }

  // end of stream signal
  static class EndOfStream {
    static EndOfStream get() {
      return new EndOfStream();
    }
  }

  private static class EndOfStreamException extends Exception {}

  static final class PartitionSupplierStream<T>
      implements Supplier
  {
    final Reader<T> reader;
    final Partition partition;
    PartitionSupplierStream(Partition<T> partition) {
      this.partition = partition;
      try {
        this.reader = partition.openReader();
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to open reader for partition: " + partition, e);
      }
    }
    @Override
    public Object get() throws EndOfStreamException {
      if (!this.reader.hasNext()) {
        try {
          this.reader.close();
        } catch (IOException e) {
          throw new RuntimeException(
              "Failed to close reader for partition: " + this.partition);
        }
        throw new EndOfStreamException();
      }
      T next = this.reader.next();
      BatchWindowing.BatchWindow w =
          BatchWindowing.get().assignWindows(next).iterator().next();
      return new Datum<>(w.getGroup(), w.getLabel(), next);
    }
  }

  /** Partitioned provider of input data for single operator. */
  private static final class InputProvider<T> implements Iterable<Supplier<T>> {
    final List<Supplier<T>> suppliers;
    InputProvider() {
      this.suppliers = new ArrayList<>();
    }

    public int size() {
      return suppliers.size();
    }

    public void add(Supplier<T> s) {
      suppliers.add(s);
    }

    public Supplier<T> get(int i) {
      return suppliers.get(i);
    }

    @Override
    public Iterator<Supplier<T>> iterator() {
      return suppliers.iterator();
    }

    Stream<Supplier<T>> stream() {
      return suppliers.stream();
    }
  }


  static class QueueCollector<T> implements Collector<T> {
    static <T> QueueCollector<T> wrap(BlockingQueue<T> queue) {
      return new QueueCollector<>(queue);
    }
    private final BlockingQueue<T> queue;
    QueueCollector(BlockingQueue<T> queue) {
      this.queue = queue;
    }
    @Override
    public void collect(T elem) {
      try {
        queue.put(elem);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private static final class QueueSupplier<T> implements Supplier<T> {

    static <T> QueueSupplier<T> wrap(BlockingQueue<T> queue) {
      return new QueueSupplier<>(queue);
    }

    private final BlockingQueue<T> queue;

    QueueSupplier(BlockingQueue<T> queue) {
      this.queue = queue;
    }

    @Override
    public T get() throws EndOfStreamException {
      try {
        T take = queue.take();
        if (take instanceof EndOfStream) {
          throw new EndOfStreamException();
        }
        return take;
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }

  }


  private static final class ExecutionContext {
    // map of operator inputs to suppliers
    Map<Pair<Operator<?, ?>, Operator<?, ?>>, InputProvider<?>> materializedOutputs
        = Collections.synchronizedMap(new HashMap<>());
    // already running operators
    Set<Operator<?, ?>> runningOperators = Collections.synchronizedSet(
        new HashSet<>());

    private boolean containsKey(Pair<Operator<?, ?>, Operator<?, ?>> d) {
      return materializedOutputs.containsKey(d);
    }
    void add(Operator<?, ?> source, Operator<?, ?> target,
        InputProvider<?> partitions) {
      Pair<Operator<?, ?>, Operator<?, ?>> edge = Pair.of(source, target);
      if (containsKey(edge)) {
        throw new IllegalArgumentException("Dataset for edge "
            + edge + " is already materialized!");
      }
      materializedOutputs.put(edge, partitions);
    }
    InputProvider<?> get(Operator<?, ?> source, Operator<?, ?> target) {
      Pair<Operator<?, ?>, Operator<?, ?>> edge = Pair.of(source, target);
      InputProvider<?> sup = materializedOutputs.get(edge);
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
      60,
      TimeUnit.SECONDS,
      queue,
      new ThreadFactory() {
        ThreadFactory factory = Executors.defaultThreadFactory();
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = factory.newThread(r);
          thread.setUncaughtExceptionHandler((Thread t, Throwable e) -> {
            e.printStackTrace(System.err);
          });
          return thread;
        }
      });

  private Triggering triggering = new ProcessingTimeTriggering();

  @Override
  public Future<Integer> submit(Flow flow) {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  @SuppressWarnings("unchecked")
  public int waitForCompletion(Flow flow) {


    // transform the given flow to DAG of basic dag
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
    for (Future f : runningTasks) {
      try {
        f.get();
      } catch (InterruptedException e) {
        break;
      } catch (ExecutionException e) {
        // when any one of the tasks fails rollback all sinks and fail
        sinks.forEach(DataSink::rollback);
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

    return 0;
  }

  /** Read all outputs of given nodes and store them using their sinks. */
  @SuppressWarnings("unchecked")
  private List<Future> consumeOutputs(
      Collection<Node<Operator<?, ?>>> leafs,
      ExecutionContext context) {
    
    List<Future> tasks = new ArrayList<>();
    // consume outputs
    for (Node<Operator<?, ?>> output : leafs) {
      DataSink<?> sink = output.get().output().getOutputSink();
      final InputProvider<?> provider = context.get(output.get(), null);
      int part = 0;
      for (Supplier<?> s : provider) {
        final Writer writer = sink.openWriter(part++);
        tasks.add(executor.submit(() -> {
          try {
            try {
              for (;;) {
                Object elem = s.get();
                // ~ swallow EndOfWindow events from leaving
                // the inmem executor
                if (elem instanceof EndOfWindow) {
                  continue;
                }
                // ~ unwrap the bare bone element from the inmem
                // specific "Datum" cargo object
                Datum datum = (Datum) elem;
                writer.write(datum.element);
              }
            } catch (EndOfStreamException ex) {
              // end of the stream
              writer.commit();
              writer.close();
              // and terminate the thread
            }
          } catch (IOException ex) {
            try {
              writer.rollback();
              // propagate exception
              throw new RuntimeException(ex);
            } catch (IOException ioex) {
              LOG.warn("Something went wrong", ioex);
              // swallow exception
            }
            throw new RuntimeException(ex);
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
  

  private InputProvider<?> createStream(DataSource<?> source) {
    InputProvider<?> ret = new InputProvider<>();
    source.getPartitions().stream()
        .map(PartitionSupplierStream::new)
        .forEach(ret::add);
    return ret;
  }


  private void execUnit(ExecUnit unit, ExecutionContext context) {
    unit.getDAG().bfs().forEach(n -> execNode(n, context));
  }

  /**
   * Execute single operator and return the suppliers for partitions
   * of output.
   */
  @SuppressWarnings("unchecked")
  private void execNode(
      Node<Operator<?, ?>> node, ExecutionContext context) {
    Operator<?, ?> op = node.get();
    final InputProvider<?> output;
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
        InputProvider<?> forkedProvider = new InputProvider<>();
        forkedProviders.add(forkedProviderQueue);
        for (int p = 0; p < output.size(); p++) {
          BlockingQueue<?> queue = new ArrayBlockingQueue<>(5000);
          forkedProviderQueue.add(queue);
          forkedProvider.add((Supplier) QueueSupplier.wrap(queue));
        }
        context.add(node.get(), ch.get(), forkedProvider);
      }
      for (int p = 0; p < output.size(); p++) {
        int partId = p;
        Supplier<?> partSup = output.get(p);
        List<BlockingQueue<?>> outputs = forkedProviders.stream()
            .map(l -> l.get(partId)).collect(Collectors.toList());
        executor.execute(() -> {
          // copy the original data to all queues
          for (;;) {
            try {
              Object item = partSup.get();
              for (BlockingQueue ch : outputs) {
                try {
                  ch.put(item);
                } catch (InterruptedException ex) {
                  return;
                }
              }
            } catch (EndOfStreamException ex) {
              for (BlockingQueue ch : outputs) {
                try {
                  ch.put(EndOfStream.get());
                } catch (InterruptedException e) {
                  // ignore
                }
              }
              return;
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
  private InputProvider<?> execMap(Node<FlatMap> flatMap,
      ExecutionContext context) {
    InputProvider<?> suppliers = context.get(
        flatMap.getSingleParentOrNull().get(), flatMap.get());
    InputProvider<?> ret = new InputProvider<>();
    final UnaryFunctor mapper = flatMap.get().getFunctor();
    for (Supplier<?> s : suppliers) {
      final BlockingQueue<?> out = new ArrayBlockingQueue(5000);
      ret.add((Supplier) QueueSupplier.wrap(out));
      executor.execute(() -> {
        QueueCollector outQ = QueueCollector.wrap(out);
        DatumCollector outC = new DatumCollector(outQ);
        try {
          for (;;) {
            // read input
            Object o = s.get();
            if (o instanceof EndOfWindow) {
              outQ.collect(o);
            } else {
              Datum d = (Datum) o;
              // transform
              outC.assignWindowing(d.group, d.label);
              mapper.apply(d.element, outC);
            }
          }
        } catch (EndOfStreamException ex) {
          outQ.collect(EndOfStream.get());
        }
      });
    }
    return ret;
  }


  @SuppressWarnings("unchecked")
  private InputProvider<?> execRepartition(
      Node<Repartition> repartition,
      ExecutionContext context) {

    Partitioning partitioning = repartition.get().getPartitioning();
    int numPartitions = partitioning.getNumPartitions();
    InputProvider<?> input = context.get(
        repartition.getSingleParentOrNull().get(), repartition.get());
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("Cannot repartition input to "
          + numPartitions + " partitions");
    }

    List<BlockingQueue> outputQueues = repartitionSuppliers(
        input, e -> e, partitioning);
    
    InputProvider<?> ret = new InputProvider<>();
    outputQueues.stream()
        .map(QueueSupplier::new)
        .forEach(s -> ret.add((Supplier) s));
    return ret;
  }


  private static final class CompositeKey<A, B> {
    final A first;
    final B second;

    CompositeKey(A first, B second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public String toString() {
      return "CompositeKey(" + first + "," + second + ")";
    }
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof CompositeKey) {
        CompositeKey other = (CompositeKey) obj;
        return first.equals(other.first) && second.equals(other.second);
      }
      return false;
    }
    @Override
    public int hashCode() {
      if (first != null && second != null) {
        int h = first.hashCode();
        int shift = Integer.SIZE >> 1;
        return ((h >> shift) | (h << shift)) ^ second.hashCode();
      }
      if (first != null) {
        return first.hashCode();
      }
      if (second != null) {
        return second.hashCode();
      }
      return 0;
    }
  }

  @SuppressWarnings("unchecked")
  private InputProvider<?> execReduceStateByKey(
      Node<ReduceStateByKey> reduceStateByKeyNode,
      ExecutionContext context) {

    final UnaryFunction keyExtractor;
    final ReduceStateByKey reduceStateByKey = reduceStateByKeyNode.get();
    if (reduceStateByKey.isGrouped()) {
      UnaryFunction reduceKeyExtractor = reduceStateByKey.getKeyExtractor();
      keyExtractor = (UnaryFunction<Pair, CompositeKey>) (Pair p) -> {
        return new CompositeKey(p.getFirst(), reduceKeyExtractor.apply(p));
      };
    } else {
      keyExtractor = reduceStateByKey.getKeyExtractor();
    }

    InputProvider<?> suppliers = context.get(
        reduceStateByKeyNode.getSingleParentOrNull().get(),
        reduceStateByKeyNode.get());

    final UnaryFunction valueExtractor = reduceStateByKey.getValueExtractor();
    final UnaryFunction stateFactory = reduceStateByKey.getStateFactory();
    final Partitioning partitioning = reduceStateByKey.getPartitioning();
    final Windowing windowing = reduceStateByKey.getWindowing();
    final CombinableReduceFunction stateCombiner = reduceStateByKey.getStateCombiner();

    List<BlockingQueue> repartitioned =
        repartitionSuppliers(suppliers, keyExtractor, partitioning);

    EndOfWindowBroadcast eowBroadcast =
        // ~ no need for broadcasts upon "batched and attached windowing"
        windowing == null || windowing == BatchWindowing.get()
            ? new EndOfWindowBroadcast.NoopInstance()
            : new EndOfWindowBroadcast.NotifyingInstance();

    InputProvider<?> outputSuppliers = new InputProvider<>();
    // consume repartitioned suppliers
    int i = 0;
    for (BlockingQueue q : repartitioned) {
      final BlockingQueue output = new ArrayBlockingQueue(5000);
      outputSuppliers.add(QueueSupplier.wrap(output));
      executor.execute(new ReduceStateByKeyReducer(
          Integer.toHexString(System.identityHashCode(reduceStateByKey))
              + "@" + reduceStateByKey.getName() + ":" + i + "/" + repartitioned.size(),
          q, output, windowing,
          keyExtractor, valueExtractor, stateFactory, stateCombiner,
          SerializableUtils.cloneSerializable(triggering),
          reduceStateByKey.input().isBounded(), eowBroadcast));
      i++;
    }
    return outputSuppliers;
  }

  @SuppressWarnings("unchecked")
  private List<BlockingQueue> repartitionSuppliers(
      InputProvider<?> suppliers,
      final UnaryFunction keyExtractor,
      final Partitioning partitioning) {

    int numInputPartitions = suppliers.size();
    final int outputPartitions = partitioning.getNumPartitions() > 0
        ? partitioning.getNumPartitions() : numInputPartitions;
    final List<BlockingQueue> ret = new ArrayList(outputPartitions);
    for (int i = 0; i < outputPartitions; i++) {
      ret.add(new ArrayBlockingQueue(5000));
    }

    // count running partition readers
    CountDownLatch workers = new CountDownLatch(numInputPartitions);

    // track end-of-window occurrences
    EndOfWindowCountDown eowCounter = new EndOfWindowCountDown();

    for (Supplier s : suppliers) {
      executor.execute(() -> {
        try {
          try {
            for (;;) {
              // read input
              Object o = s.get();
              if (o instanceof EndOfWindow) {
                if (eowCounter.countDown((EndOfWindow) o, numInputPartitions)) {
                  for (BlockingQueue r : ret) {
                    r.put(o);
                  }
                }
              } else {
                Datum d = (Datum) o;
                // determine partition
                Object key = keyExtractor.apply(d.element);
                int partition
                    = (partitioning.getPartitioner().getPartition(key) & Integer.MAX_VALUE)
                      % outputPartitions;
                // write to the right partition
                ret.get(partition).put(d);
              }
            }
          } catch (EndOfStreamException ex) {
            // ~ no-op
          }
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        } finally {
          workers.countDown();
        }
      });
    }

    waitForStreamEnds(workers, ret);

    return ret;
  }


  // wait until runningTasks is not zero and then send EOF to all output queues
  @SuppressWarnings("unchecked")
  private void waitForStreamEnds(
      CountDownLatch fire, List<BlockingQueue> outputQueues) {
    // start a new task that will wait for all read partitions to end
    executor.execute(() -> {
      try {
        fire.await();
      } catch (InterruptedException ex) {
        LOG.warn("waiting-for-stream-ends interrupted");
      }
      // try sending eof to all outputs
      for (BlockingQueue queue : outputQueues) {
        try {
          queue.put(EndOfStream.get());
        } catch (InterruptedException ex) {
          // nop
        }
      }
    });
  }

  
  @SuppressWarnings("unchecked")
  private InputProvider<?> execUnion(
      Node<Union> union, ExecutionContext context) {

    InputProvider<?> ret = new InputProvider<>();
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


  public InMemExecutor setTriggering(Triggering triggering) {
    this.triggering = triggering;
    return this;
  }
  
}
