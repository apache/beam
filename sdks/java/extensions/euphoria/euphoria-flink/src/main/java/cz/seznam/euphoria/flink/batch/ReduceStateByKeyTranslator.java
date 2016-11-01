package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.flink.batch.greduce.GroupReducer;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.Set;

public class ReduceStateByKeyTranslator implements BatchOperatorTranslator<ReduceStateByKey> {

  final StorageProvider stateStorageProvider;

  public ReduceStateByKeyTranslator(Settings settings, ExecutionEnvironment env) {
    int maxMemoryElements = settings.getInt(CFG_MAX_MEMORY_ELEMENTS_KEY, CFG_MAX_MEMORY_ELEMENTS_DEFAULT);
    this.stateStorageProvider = new BatchStateStorageProvider(maxMemoryElements, env);
  }

  @Override
  @SuppressWarnings("unchecked")
  public DataSet translate(FlinkOperator<ReduceStateByKey> operator,
                           BatchExecutorContext context) {

    // FIXME #16800 - parallelism should be set to the same level as parent until we reach "shuffling"

    DataSet input = Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceStateByKey origOperator = operator.getOriginalOperator();

    final Windowing windowing =
        origOperator.getWindowing() == null
            ? AttachedWindowing.INSTANCE
            : origOperator.getWindowing();

    final UnaryFunction udfKey;
    final UnaryFunction udfValue;
    if (origOperator.isGrouped()) {
      UnaryFunction kfn = origOperator.getKeyExtractor();
      udfKey = (UnaryFunction<Pair, CompositeKey>)
              (Pair p) -> CompositeKey.of(p.getFirst(), kfn.apply(p.getSecond()));
      UnaryFunction vfn = origOperator.getValueExtractor();
      udfValue = (UnaryFunction<Pair, Object>) (Pair p) -> vfn.apply(p.getSecond());
    } else {
      udfKey = origOperator.getKeyExtractor();
      udfValue = origOperator.getValueExtractor();
    }

    // ~ re-assign element timestamps
    if (windowing.getTimestampAssigner().isPresent()) {
      UnaryFunction<Object, Long> timestampAssigner =
          (UnaryFunction<Object, Long>) windowing.getTimestampAssigner().get();
      MapOperator<Object, StampedWindowElement> tsAssigned =
          input.<StampedWindowElement>map((Object value) -> {
            WindowedElement we = (WindowedElement) value;
            long ts = timestampAssigner.apply(we.get());
            return new StampedWindowElement<>(we.getWindow(), we.get(), ts);
          });
      input = tsAssigned
          .name(operator.getName() + "::assign-timestamps")
          .setParallelism(operator.getParallelism())
          .returns((Class) StampedWindowElement.class);
    } else {
      // ~ FIXME #16648 - make sure we're dealing with StampedWindowedElements; we can
      // drop this once only such elements are floating throughout the whole batch executor
      MapOperator<Object, StampedWindowElement> mapped =
          input.map((MapFunction) value -> {
            WindowedElement we = (WindowedElement) value;
            if (we instanceof StampedWindowElement) {
              return we;
            }
            return new StampedWindowElement<>(we.getWindow(), we.get(), Long.MAX_VALUE);
          });
      input = mapped.name(operator.getName() + "::make-stamped-windowed-elements")
          .setParallelism(operator.getParallelism())
          .returns((Class) StampedWindowElement.class);
    }

    // ~ extract key/value from input elements and assign windows
    DataSet<StampedWindowElement> tuples;
    {
      // FIXME require keyExtractor to deliver `Comparable`s

      FlatMapOperator<Object, StampedWindowElement> wAssigned =
          input.flatMap((i, c) -> {
            StampedWindowElement wel = (StampedWindowElement) i;
            Set<Window> assigned = windowing.assignWindowsToElement(wel);
            for (Window wid : assigned) {
              Object el = wel.get();
              c.collect(new StampedWindowElement(
                  wid,
                  Pair.of(udfKey.apply(el), udfValue.apply(el)),
                  wel.getTimestamp()));
            }
          });
      tuples = wAssigned
          .name(operator.getName() + "::map-input")
          .setParallelism(operator.getParallelism())
          .returns((Class) StampedWindowElement.class);
    }

    // ~ reduce the data now
    DataSet<StampedWindowElement<?, Pair>> reduced =
        tuples.groupBy((KeySelector)
            Utils.wrapQueryable(
                // ~ FIXME if the underlying windowing is "non merging" we can group by
                // "key _and_ window", thus, better utilizing the available resources
                (StampedWindowElement<?, Pair> we) -> (Comparable) we.get().getFirst(),
                Comparable.class))
            .sortGroup((KeySelector) Utils.wrapQueryable(
                (KeySelector<StampedWindowElement<?, ?>, Long>)
                    StampedWindowElement::getTimestamp, Long.class),
                Order.ASCENDING)
            .reduceGroup(new RSBKReducer(origOperator, stateStorageProvider, windowing))
            .setParallelism(operator.getParallelism())
            .name(operator.getName() + "::reduce");

    // apply custom partitioner if different from default HashPartitioner
    if (!(origOperator.getPartitioning().getPartitioner().getClass() == HashPartitioner.class)) {
      reduced = reduced
          .partitionCustom(new PartitionerWrapper<>(
              origOperator.getPartitioning().getPartitioner()),
              Utils.wrapQueryable(
                  (KeySelector<StampedWindowElement<?, Pair>, Comparable>)
                      (StampedWindowElement<?, Pair> we) -> (Comparable) we.get().getKey(),
                  Comparable.class))
          .setParallelism(operator.getParallelism());
    }

    return reduced;
  }

  static class RSBKReducer
          implements GroupReduceFunction<StampedWindowElement<?, Pair>, StampedWindowElement<?, Pair>>,
          ResultTypeQueryable<StampedWindowElement<?, Pair>>
  {
    private final StateFactory<?, State> stateFactory;
    private final CombinableReduceFunction<State> stateCombiner;
    private final StorageProvider stateStorageProvider;
    private final Windowing windowing;
    private final Trigger trigger;

    RSBKReducer(
        ReduceStateByKey operator,
        StorageProvider stateStorageProvider,
        Windowing windowing) {

      this.stateFactory = operator.getStateFactory();
      this.stateCombiner = operator.getStateCombiner();
      this.stateStorageProvider = stateStorageProvider;
      this.windowing = windowing;
      this.trigger = windowing.getTrigger();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void reduce(Iterable<StampedWindowElement<?, Pair>> values,
                       org.apache.flink.util.Collector<StampedWindowElement<?, Pair>> out)
    {
      GroupReducer reducer = new GroupReducer(
          stateFactory,
          stateCombiner,
          stateStorageProvider,
          windowing,
          trigger,
          elem -> out.collect((StampedWindowElement) elem));
      for (StampedWindowElement value : values) {
        reducer.process(value);
      }
      reducer.close();
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<StampedWindowElement<?, Pair>> getProducedType() {
      return TypeInformation.of((Class) StampedWindowElement.class);
    }
  }
}
