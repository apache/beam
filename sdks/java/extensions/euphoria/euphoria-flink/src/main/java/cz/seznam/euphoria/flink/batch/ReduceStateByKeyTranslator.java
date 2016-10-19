package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.flink.functions.ComparablePair;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.Iterator;
import java.util.Set;

import org.apache.flink.api.java.ExecutionEnvironment;

public class ReduceStateByKeyTranslator implements BatchOperatorTranslator<ReduceStateByKey> {

  final static String CFG_MAX_MEMORY_ELEMENTS = "euphoria.flink.batch.state.max.memory.elements";
  
  final StorageProvider stateStorageProvider;

  public ReduceStateByKeyTranslator(Settings settings, ExecutionEnvironment env) {
    int maxMemoryElements = settings.getInt(CFG_MAX_MEMORY_ELEMENTS, 1000);
    this.stateStorageProvider = new BatchStateStorageProvider(maxMemoryElements, env);
  }

  @Override
  @SuppressWarnings("unchecked")
  public DataSet translate(FlinkOperator<ReduceStateByKey> operator,
                           BatchExecutorContext context)
  {
    DataSet<?> input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceStateByKey origOperator = operator.getOriginalOperator();

    final StateFactory<?, State> stateFactory = origOperator.getStateFactory();
    final Windowing windowing =
        origOperator.getWindowing() == null
            ? AttachedWindowing.INSTANCE
            : origOperator.getWindowing();

    final UnaryFunction udfKey;
    final UnaryFunction udfValue;
    if (origOperator.isGrouped()) {
      UnaryFunction reduceKeyExtractor = origOperator.getKeyExtractor();
      udfKey = (UnaryFunction<Pair, CompositeKey>)
              (Pair p) -> CompositeKey.of(
                      p.getFirst(),
                      reduceKeyExtractor.apply(p.getSecond()));
      UnaryFunction vfn = origOperator.getValueExtractor();
      udfValue = (UnaryFunction<Pair, Object>)
              (Pair p) -> vfn.apply(p.getSecond());
    } else {
      udfKey = origOperator.getKeyExtractor();
      udfValue = origOperator.getValueExtractor();
    }

    DataSet tuples = (DataSet) input.flatMap((i, c) -> {
          WindowedElement wel = (WindowedElement) i;
          Set<Window> windows = windowing.assignWindowsToElement(wel);
          for (Window window : windows) {
            Object el = wel.get();
            c.collect(new WindowedElement(
                window,
                Pair.of(udfKey.apply(el), udfValue.apply(el))));
          }
        })
        .name(operator.getName() + "::map-input")
        // FIXME parallelism should be set to the same level as parent
        // since this "map-input" transformation is applied before shuffle
        .setParallelism(operator.getParallelism())
        .returns((Class) WindowedElement.class);

    // TODO in case of merging widows elements should be sorted by window label first
    // and windows having the same label should be given chance to merge
    // FIXME require keyExtractor to deliver `Comparable`s
    DataSet<WindowedElement<?, Pair>> reduced;
    reduced = tuples.groupBy(new TypedKeySelector<>())
        .reduceGroup(
            new TypedReducer(origOperator, stateFactory, stateStorageProvider))
        .setParallelism(operator.getParallelism())
        .name(operator.getName() + "::reduce");

    // apply custom partitioner if different from default HashPartitioner
    if (!(origOperator.getPartitioning().getPartitioner().getClass() == HashPartitioner.class)) {
      reduced = reduced
          .partitionCustom(new PartitionerWrapper<>(
              origOperator.getPartitioning().getPartitioner()),
              Utils.wrapQueryable(
                  (WindowedElement<?, Pair> we) -> (Comparable) we.get().getKey(),
                  Comparable.class))
          .setParallelism(operator.getParallelism());
    }

    return reduced;
  }

  private static class TypedKeySelector<WID extends Window, KEY>
      implements KeySelector<WindowedElement<WID, ? extends Pair<KEY, ?>>, ComparablePair<WID, KEY>>,
      ResultTypeQueryable<ComparablePair<WID, KEY>>
  {
    @Override
    public ComparablePair<WID, KEY> getKey(WindowedElement<WID, ? extends Pair<KEY, ?>> value) {
      return ComparablePair.of(value.getWindow(), value.get().getKey());
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<ComparablePair<WID, KEY>> getProducedType() {
      return TypeInformation.of((Class) ComparablePair.class);
    }
  }

  private static class TypedReducer
          implements GroupReduceFunction<WindowedElement<?, Pair>, WindowedElement<?, Pair>>,
          ResultTypeQueryable<WindowedElement<?, Pair>>
  {
    private final Operator<?, ?> operator;
    private final StateFactory<?, State> stateFactory;
    private final StorageProvider stateStorageProvider;

    public TypedReducer(
        Operator<?, ?> operator,
        StateFactory<?, State> stateFactory,
        StorageProvider stateStorageProvider) {

      this.operator = operator;
      this.stateFactory = stateFactory;
      this.stateStorageProvider = stateStorageProvider;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void reduce(Iterable<WindowedElement<?, Pair>> values,
                       org.apache.flink.util.Collector<WindowedElement<?, Pair>> out)
    {
      Iterator<WindowedElement<?, Pair>> it = values.iterator();

      // read the first element to obtain window metadata and key
      WindowedElement<?, Pair> element = it.next();
      final Window wid = element.getWindow();
      final Object key = element.get().getKey();

      State state = stateFactory.apply(
          new Context() {
            @Override
            public void collect(Object elem) {
              out.collect(new WindowedElement<>(wid, Pair.of(key, elem)));
            }
            @Override
            public Object getWindow() {
              return wid;
            }
          },
          stateStorageProvider);

      // add the first element to the state
      state.add(element.get().getValue());

      while (it.hasNext()) {
        state.add(it.next().get().getValue());
      }

      state.flush();
      state.close();
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<WindowedElement<?, Pair>> getProducedType() {
      return TypeInformation.of((Class) WindowedElement.class);
    }
  }
}
