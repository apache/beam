
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
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
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.State;
import cz.seznam.euphoria.core.client.operator.Union;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
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
  private static class EndOfStream {
    private static EndOfStream get() {
      return new EndOfStream();
    }
  }

  private static class EndOfStreamException extends Exception {}

  private static final class PartitionSupplierStream<T> implements Supplier<T> {
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
    public T get() throws EndOfStreamException {
      if (!this.reader.hasNext()) {
        try {
          this.reader.close();
        } catch (IOException e) {
          throw new RuntimeException(
              "Failed to close reader for partition: " + this.partition);
        }
        throw new EndOfStreamException();
      }
      return this.reader.next();
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
    // map of operator inputs to suppliers
    Map<Pair<Operator<?, ?, ?>, Operator<?, ?, ?>>, InputProvider<?>> materializedOutputs
        = Collections.synchronizedMap(new HashMap<>());

    private boolean containsKey(Pair<Operator<?, ?, ?>, Operator<?, ?, ?>> d) {
      return materializedOutputs.containsKey(d);
    }
    void add(Operator<?, ?, ?> source, Operator<?, ?, ?> target,
        InputProvider<?> partitions) {
      Pair<Operator<?, ?, ?>, Operator<?, ?, ?>> edge = Pair.of(source, target);
      if (containsKey(edge)) {
        throw new IllegalArgumentException("Dataset for edge "
            + edge + " is already materialized!");
      }
      materializedOutputs.put(edge, partitions);
    }
    InputProvider<?> get(Operator<?, ?, ?> source, Operator<?, ?, ?> target) {
      Pair<Operator<?, ?, ?>, Operator<?, ?, ?>> edge = Pair.of(source, target);
      InputProvider<?> sup = materializedOutputs.get(edge);
      if (sup == null) {
        throw new IllegalArgumentException(
            "Do not have suppliers for given edge (original producer "
            + source.output().getProducer() + ")");
      }
      return sup;
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

  @Override
  public Future<Integer> submit(Flow flow) {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  @SuppressWarnings("unchecked")
  public int waitForCompletion(Flow flow) {
    ExecutionContext context = new ExecutionContext();

    // transform the given flow to DAG of basic dag
    DAG<Operator<?, ?, ?>> dag = FlowUnfolder.unfold(flow, Executor.getBasicOps());

    final List<Future> runningTasks = new ArrayList<>();
    Collection<Node<Operator<?, ?, ?>>> leafs = dag.getLeafs();

    // create record for each input dataset in context
    // input datasets are represented as edges with null parent
    dag.bfs()
        .flatMap(n -> n.get().listInputs().stream()
            // take input datasets
            .filter(i -> i.getProducer() == null)
            .map(i -> Pair.of(n, i)))
        .forEach(p -> {
          Dataset<?> inputDataset = p.getSecond();
          context.add(
              null, p.getFirst().get(), createStream(inputDataset.getSource()));
        });

    List<ExecUnit> units = ExecUnit.split(dag);
    for (ExecUnit unit : units) {
      execUnit(unit, context);
    }
    // consume outputs
    for (Node<Operator<?, ?, ?>> output : leafs) {
      DataSink<?> sink = output.get().output().getOutputSink();
      final InputProvider<?> provider = context.get(output.get(), null);
      int part = 0;
      for (Supplier<?> s : provider) {
        final Writer writer = sink.openWriter(part++);
        runningTasks.add(executor.submit(() -> {
          try {
            try {
              for (;;) {
                writer.write(s.get());
              }
            } catch (EndOfStreamException ex) {
              // end of the stream
              writer.commit();
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

  @SuppressWarnings("unchecked")
  private InputProvider<?> createStream(DataSource<?> source) {
    InputProvider ret = new InputProvider();
    source.getPartitions().stream()
        .map(PartitionSupplierStream::new)
        .forEach(ret::add);
    return ret;
  }


  private void execUnit(ExecUnit unit, ExecutionContext context) {
    for (ExecPath path : unit.getPaths()) {
      execDAG(path.dag(), context);
    }
  }


  private void execDAG(
      DAG<Operator<?, ?, ?>> dag, ExecutionContext context) {

    dag.bfs().forEach(n -> execNode(n, context));
  }

  /**
   * Execute single operator and return the suppliers for partitions
   * of output.
   */
  @SuppressWarnings("unchecked")
  private void execNode(
      Node<Operator<?, ?, ?>> node, ExecutionContext context) {
    Operator<?, ?, ?> op = node.get();
    final InputProvider<?> output;
    if (op instanceof FlatMap) {
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
    // store output for each child
    if (node.getChildren().size() > 1) {
      List<List<BlockingQueue<?>>> forkedProviders = new ArrayList<>();
      for (Node<Operator<?, ?, ?>> ch : node.getChildren()) {
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
            for (BlockingQueue ch : outputs) {
              try {
                ch.put(partSup.get());
              } catch (InterruptedException ex) {
                return;
              } catch (EndOfStreamException ex) {
                try {
                  ch.put(EndOfStream.get());
                } catch (InterruptedException e) {
                  // ignore
                }
                return;
              }
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
      final BlockingQueue<?> blockingQueue = new ArrayBlockingQueue(50);
      QueueSupplier<?> outputSupplier = QueueSupplier.wrap(blockingQueue);
      ret.add((Supplier) outputSupplier);
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
  private InputProvider<?> execRepartition(
      Node<Repartition> repartition,
      ExecutionContext context) {

    Partitioning<?> partitioning = repartition.get().getPartitioning();
    Partitioner<?> partitioner = partitioning.getPartitioner();
    int numPartitions = partitioning.getNumPartitions();
    InputProvider<?> input = context.get(
        repartition.getSingleParentOrNull().get(), repartition.get());
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
      return first.hashCode() ^ second.hashCode();
    }
  }

  @SuppressWarnings("unchecked")
  private InputProvider<?> execReduceStateByKey(
      Node<ReduceStateByKey> reduceStateByKeyNode,
      ExecutionContext context) {

    final UnaryFunction keyExtractor;
    final ReduceStateByKey<?, ?, ?, ?, ?, ?, ?, ?, ?> reduceStateByKey
        = reduceStateByKeyNode.get();
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
    final BinaryFunction stateFactory = reduceStateByKey.getStateFactory();
    final Partitioning partitioning = reduceStateByKey.getPartitioning();
    final int outputPartitions = partitioning.getNumPartitions() > 0
        ? partitioning.getNumPartitions() : suppliers.size();
    final Windowing windowing = reduceStateByKey.getWindowing();

    final List<BlockingQueue> repartitioned = new ArrayList(outputPartitions);
    for (int i = 0; i < outputPartitions; i++) {
      repartitioned.add(new ArrayBlockingQueue(10));
    }

    
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

    InputProvider<?> outputSuppliers = new InputProvider<>();

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


  @SuppressWarnings("unchecked")
  private InputProvider<?> execUnion(
      Node<Union> union, ExecutionContext context) {

    InputProvider<?> ret = new InputProvider<>();
    union.getParents().stream()
        .flatMap(p -> context.get(p.get(), union.get()).stream())
        .forEach(s -> ret.add((Supplier) s));
    return ret;
  }


}
