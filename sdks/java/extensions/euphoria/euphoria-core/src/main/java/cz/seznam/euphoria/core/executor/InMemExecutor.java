
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.operator.Pair;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.State;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Inmem executor for testing.
 */
public class InMemExecutor implements Executor {


  @FunctionalInterface
  private interface Supplier<T> {
    T get() throws EndOfStreamException;
  }

  // end of stream signal
  private static class EndOfStream {
    private static EndOfStream get() {
      return new EndOfStream();
    }
  }

  private static class EndOfStreamException extends Exception {
    
  }

  
  private static final class PartitionSupplierStream<T> implements Supplier<T> {
    final Iterator<T> it;
    PartitionSupplierStream(Partition<T> partition) {
      this.it = partition.iterator();
    }
    @Override
    public T get() throws EndOfStreamException {
      T next = it.next();
      if (next == null) {
        throw new EndOfStreamException();
      }
      return next;
    }
  }


  private static final class QueueCollector<T> implements Collector<T> {
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
    Map<Dataset<?>, List<Supplier<?>>> materializedDatasets
        = Collections.synchronizedMap(new HashMap<>());

    private boolean containsKey(Dataset<?> d) {
      return materializedDatasets.containsKey(d);
    }
    void add(Dataset<?> dataset, List<Supplier<?>> partitions) {
      if (containsKey(dataset)) {
        throw new IllegalArgumentException("Dataset "
            + dataset + " is already materialized!");
      }
      materializedDatasets.put(dataset, partitions);
    }
    List<Supplier<?>> get(Dataset<?> ds) {
      List<Supplier<?>> sup = materializedDatasets.get(ds);
      if (sup == null) {
        throw new IllegalArgumentException(
            "Do not have suppliers for given dataset (original producer "
            + ds.getProducer() + ")");
      }
      return sup;
    }
  }

  private final BlockingQueue<Runnable> queue = new LinkedTransferQueue<>();
  private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
      Runtime.getRuntime().availableProcessors(),
      10 * Runtime.getRuntime().availableProcessors(),
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

  @Override
  public Future<Integer> submit(Flow flow) {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  @SuppressWarnings("unchecked")
  public int waitForCompletion(Flow flow) {
    ExecutionContext context = new ExecutionContext();
    Collection<Operator<?, ?, ?>> operators = flow.operators();
    Collection<Dataset<?>> sources = flow.sources();
    Collection<Dataset<?>> outputs = operators.stream()
        .map(o -> (Dataset<?>) o.output())
        .filter(d -> d.getOutputSink() != null)
        .collect(Collectors.toList());

    sources.stream()
        .forEach(d -> context.add(d, createStream(d)));
    List<ExecUnit> units = ExecUnit.split(flow);
    for (ExecUnit unit : units) {
      execUnit(unit, context);
    }
    // consume outputs
    for (Dataset<?> o : outputs) {
      DataSink<?> sink = o.getOutputSink();
      int part = 0;
      for (Supplier s : context.get(o)) {
        final Writer writer = sink.openWriter(part++);
        executor.execute(() -> {
          try {
            try {
              for (;;) {
                writer.write(s.get());
              }
            } catch (EndOfStreamException ex) {
              // end of the stream
              writer.commit();
              writer.close();
              // and terminate the thread
            }
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        });
      }
    }
    while (executor.getActiveCount() != 0) {
      try {
        executor.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException ex) {
        break;
      }
    }
    return 0;

  }

  @SuppressWarnings("unchecked")
  private List<Supplier<?>> createStream(Dataset<?> s) {
    final DataSource<?> source = s.getSource();
    if (source == null) {
      throw new IllegalStateException("Can create streams only on inputs.");
    }
    return (List) source.getPartitions().stream()
        .map(PartitionSupplierStream::new)
        .collect(Collectors.toList());
  }


  private void execUnit(ExecUnit unit, ExecutionContext context) {
    for (ExecPath path : unit.getPaths()) {
      validateDAG(path.operators());
      execDAG(path.operators(), context);
    }
  }

  /** Validate that this runtime is capable of running this DAG. */
  private void validateDAG(DAG<Operator<?, ?, ?>> operators) {
    final Set<Class<?>> supported = Executor.getBasicOps();

    operators.nodes()
        .filter(o -> !supported.contains(o.getClass()))
        .map(o -> {
          DAG<Operator<?, ?, ?>> basicOps = o.getBasicOps();
          if (basicOps.size() == 1) {
            // we have to ensure that this is not a self replacement
            if (basicOps.nodes().findFirst().get().getClass() == o.getClass()) {
              throw new IllegalStateException("Operator " + o + " does not correctly "
                  + "define `getBasicOps`, basicOps are " + Executor.getBasicOps());
            }
          }
          return basicOps;
        })
        .forEach(this::validateDAG);
  }


  private void execDAG(
      DAG<Operator<?, ?, ?>> operators, ExecutionContext context) {

    boolean haveAllInputs = operators.getRoots().stream()
            .map(DAG.Node::get)
            .allMatch(o -> o.listInputs()
                .stream()
                .allMatch(context::containsKey));
    
    if (!haveAllInputs) {
      throw new IllegalStateException("Cannot execute DAG "
          + operators + ", missing some inputs!");
    }
    
    // OK, we have all inputs, lets execute operations
    for (Operator<?, ?, ?> op : getExecutableOperators(operators)) {
      List<Supplier<?>> output = execOp(op, context);
      context.add(op.output(), output);
    }
  }

  /**
   * Execute single operator and return the suppliers for partitions
   * of output.
   */
  private List<Supplier<?>> execOp(
      Operator<?, ?, ?> op, ExecutionContext context) {
    if (op instanceof FlatMap) {
      return execMap((FlatMap<?, ?, ?>) op, context);
    } else if (op instanceof Repartition) {
      return execRepartition((Repartition<?, ?>) op, context);
    } else if (op instanceof ReduceStateByKey) {
      return execReduceStateByKey((ReduceStateByKey<?, ?, ?, ?, ?, ?, ?>) op, context);
    } else {
      DAG<Operator<?, ?, ?>> basicOps = op.getBasicOps();
      List<Supplier<?>> output = null;
      for (Operator<?, ?, ?> bop : getExecutableOperators(basicOps)) {
        output = execOp(bop, context);
        context.add(bop.output(), output);
      }
      return output;
    }

  }


  private List<Operator<?, ?, ?>> getExecutableOperators(DAG<Operator<?, ?, ?>> dag) {
    List<Operator<?, ?, ?>> ret = new ArrayList<>();
    final Collection<DAG.Node<Operator<?, ?, ?>>> roots = dag.getRoots();
    final Queue<DAG.Node<Operator<?, ?, ?>>> unprocessed = new LinkedList<>();

    roots.stream().forEach(unprocessed::add);
    while (!unprocessed.isEmpty()) {
      DAG.Node<Operator<?, ?, ?>> poll = unprocessed.poll();
      ret.add(poll.get());
      poll.getChildren().stream().forEach(unprocessed::add);
    }
    return ret;

  }


  @SuppressWarnings("unchecked")
  private List<Supplier<?>> execMap(FlatMap<?, ?, ?> flatMap,
      ExecutionContext context) {
    Dataset<?> input = flatMap.input();
    List<Supplier<?>> suppliers = context.get(input);
    List<Supplier<?>> ret = new ArrayList<>(suppliers.size());
    final UnaryFunctor mapper = flatMap.getFunctor();
    for (Supplier<?> s : suppliers) {
      final BlockingQueue<?> blockingQueue = new ArrayBlockingQueue(50);
      ret.add(QueueSupplier.wrap(blockingQueue));
      final Collector c = QueueCollector.wrap(blockingQueue);
      executor.execute(() -> {
        try {
          for (;;) {
            // read input
            Object o = s.get();
            // transform
            mapper.apply(o, c);
          }
        } catch (EndOfStreamException ex) {
          c.collect(EndOfStream.get());
        }
      });
    }
    return ret;
  }


  @SuppressWarnings("unchecked")
  private List<Supplier<?>> execRepartition(
      Repartition<?, ?> repartition,
      ExecutionContext context) {

    Partitioning<?> partitioning = repartition.getPartitioning();
    Partitioner<?> partitioner = partitioning.getPartitioner();
    int numPartitions = partitioning.getNumPartitions();
    List<Supplier<?>> input = context.get(repartition.input());
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("Cannot repartition input to "
          + numPartitions + " partitions");
    }
    List<BlockingQueue> outputQueues = new ArrayList<>(numPartitions);
    for (Supplier<?> s : input) {
      BlockingQueue outputQueue = new ArrayBlockingQueue(
          10 * (input.size() / numPartitions) + 1);
      outputQueues.add(outputQueue);
      executor.execute(() -> {
        try {
          for (;;) {
            outputQueue.put(s.get());
          }
        } catch (EndOfStreamException | InterruptedException ex) {
          outputQueue.add(EndOfStream.get());
        }
      });
    }
    return (List<Supplier<?>>) outputQueues.stream()
        .map(QueueSupplier::new)
        .collect(Collectors.toList());
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
      return first.hashCode() ^ second.hashCode();
    }
  }

  @SuppressWarnings("unchecked")
  private List<Supplier<?>> execReduceStateByKey(
      ReduceStateByKey<?, ?, ?, ?, ?, ?, ?> reduceStateByKey,
      ExecutionContext context) {

    final Dataset<?> input = reduceStateByKey.input();
    final UnaryFunction keyExtractor;
    if (reduceStateByKey.isGrouped()) {
      GroupedDataset grouped = (GroupedDataset) input;
      UnaryFunction reduceKeyExtractor = reduceStateByKey.getKeyExtractor();
      keyExtractor = (UnaryFunction<Pair, CompositeKey>) (Pair p) -> {
        return new CompositeKey(p.getFirst(), reduceKeyExtractor.apply(p));
      };
    } else {
      keyExtractor = reduceStateByKey.getKeyExtractor();
    }

    final UnaryFunction valueExtractor = reduceStateByKey.getValueExtractor();
    final BinaryFunction stateFactory = reduceStateByKey.getStateFactory();
    final Partitioning partitioning = reduceStateByKey.getPartitioning();
    final int outputPartitions = partitioning.getNumPartitions() > 0
        ? partitioning.getNumPartitions() : input.getPartitioning().getNumPartitions();
    final Windowing windowing = reduceStateByKey.getWindowing();

    final List<BlockingQueue> repartitioned = new ArrayList(outputPartitions);
    for (int i = 0; i < outputPartitions; i++) {
      repartitioned.add(new ArrayBlockingQueue(10));
    }

    List<Supplier<?>> suppliers = context.get(input);
    for (Supplier<?> s : suppliers) {
      // read suppliers
      executor.execute(() -> {
        try {
          try {
            for (;;) {
              Object o = s.get();
              Object key = keyExtractor.apply(o);
              int partition
                  = (partitioning.getPartitioner().getPartition(key) & Integer.MAX_VALUE)
                  % outputPartitions;
                repartitioned.get(partition).put(o);
            }
          } catch (EndOfStreamException ex) {
            repartitioned.stream().forEach(
                q -> {
                  try {
                    q.put(EndOfStream.get());
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }});
          }
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      });
    }

    List<Supplier<?>> outputSuppliers = new ArrayList<>(repartitioned.size());

    // consume repartitioned suppliers
    repartitioned.stream().forEach(q -> {
      final Map<Window, Map<Object, State>> windowStates = new HashMap<>();
      final BlockingQueue output = new ArrayBlockingQueue(10);
      final Map<Object, State> aggregatingStates = new HashMap<>();
      final boolean isAggregating = windowing.isAggregating();
      final LocalTriggering triggering = new LocalTriggering();

      outputSuppliers.add(QueueSupplier.wrap(output));
      executor.execute(() -> {
        for (;;) {
          try {
            Object item = q.take();
            if (item instanceof EndOfStream) {
              break;
            }
            Object key = keyExtractor.apply(item);
            Object value = valueExtractor.apply(item);
            final Set<Window> itemWindows;
            itemWindows = windowing.allocateWindows(
               item, triggering, new UnaryFunction<Window, Void>() {
                 @Override
                 public Void apply(Window window) {
                    window.flushAll();
                    windowStates.remove(window);
                    return null;
                 }
               });

            for (Window w : itemWindows) {
              Map<Object, State> keyStates = windowStates.get(w);
              if (keyStates == null) {
                keyStates = new HashMap<>();
                windowStates.put(w, keyStates);
              }
              State keyState = keyStates.get(key);
              if (keyState == null) {
                State aggregatingState = aggregatingStates.get(key);
                if (!isAggregating || aggregatingState == null) {
                  keyState = (State) stateFactory.apply(key, QueueCollector.wrap(output));
                  w.addState(keyState);
                  if (isAggregating) {
                    aggregatingStates.put(key, keyState);
                  }
                } else if (isAggregating) {
                  keyState = aggregatingState;
                }
                keyStates.put(key, keyState);
              }
              keyState.add(value);              
            }
          } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
        // close all states
        windowStates.values().stream()
            .flatMap(m -> m.values().stream())
            .forEach(State::close);
        output.add(EndOfStream.get());
        triggering.close();
      });
    });
    return outputSuppliers;
  }

}
