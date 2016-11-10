package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.TimedWindow;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.flink.functions.ComparablePair;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Preconditions;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.Arrays;
import java.util.Set;

public class ReduceByKeyTranslator implements BatchOperatorTranslator<ReduceByKey> {

  static boolean wantTranslate(ReduceByKey operator) {
    return operator.isCombinable()
        && (operator.getWindowing() == null
        || !(operator.getWindowing() instanceof MergingWindowing)
        || !operator.getWindowing().getTrigger().isStateful());
  }

  @Override
  @SuppressWarnings("unchecked")
  public DataSet translate(FlinkOperator<ReduceByKey> operator,
                           BatchExecutorContext context) {

    // FIXME #16800 - parallelism should be set to the same level as parent until we reach "shuffling"

    DataSet input = Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceByKey origOperator = operator.getOriginalOperator();
    final UnaryFunction<Iterable, Object> reducer = origOperator.getReducer();
    final Windowing windowing =
        origOperator.getWindowing() == null
        ? AttachedWindowing.INSTANCE
        : origOperator.getWindowing();

    Preconditions.checkState(
        !(windowing instanceof MergingWindowing),
        "MergingWindowing not supported!");
    Preconditions.checkState(!windowing.getTrigger().isStateful(),
        "Stateful triggers not supported!");

    // ~ prepare key and value functions
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

    // ~ extract key/value from input elements and assign windows
    DataSet<StampedWindowElement> tuples;
    {
      // FIXME require keyExtractor to deliver `Comparable`s

      UnaryFunction<Object, Long> timeAssigner =
          (UnaryFunction<Object, Long>) windowing.getTimestampAssigner().orElse(null);
      FlatMapOperator<Object, StampedWindowElement> wAssigned =
          input.flatMap((i, c) -> {
            StampedWindowElement wel = (StampedWindowElement) i;
            if (timeAssigner != null) {
              long stamp = timeAssigner.apply(wel.get());
              i = wel = new StampedWindowElement(wel.getWindow(), wel.get(), stamp);
            }
            Set<Window> assigned = windowing.assignWindowsToElement(wel);
            for (Window wid : assigned) {
              Object el = wel.get();
              long stamp = (wid instanceof TimedWindow)
                  ? ((TimedWindow) wid).maxTimestamp()
                  : wel.getTimestamp();
              c.collect(new StampedWindowElement(
                  wid, Pair.of(udfKey.apply(el), udfValue.apply(el)), stamp));
            }
          });
      tuples = wAssigned
          .name(operator.getName() + "::map-input")
          .setParallelism(operator.getParallelism())
          .returns((Class) StampedWindowElement.class);
    }

    // ~ reduce the data now
    Operator<StampedWindowElement<?, Pair>, ?> reduced;
    reduced = tuples
        .groupBy((KeySelector) new RBKKeySelector<>())
        .reduce(new RBKReducer(reducer));
    reduced = reduced
        .setParallelism(operator.getParallelism())
        .name(operator.getName() + "::reduce");

    // FIXME partitioner should be applied during "reduce" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "groupBy" transformation

    // apply custom partitioner if different from default HashPartitioner
    if (origOperator.getPartitioning().getPartitioner().getClass() != HashPartitioner.class) {
      reduced = reduced
          .partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()),
              Utils.wrapQueryable(
                  (KeySelector<StampedWindowElement<?, Pair>, Comparable>)
                      (StampedWindowElement<?, Pair> we) -> (Comparable) we.get().getKey(),
                  Comparable.class))
          .setParallelism(operator.getParallelism());
    }

    return reduced;
  }

  // ------------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  static class RBKKeySelector<LABEL, KEY>
      implements KeySelector<StampedWindowElement<?, ? extends Pair<KEY, ?>>,
                             ComparablePair<LABEL, KEY>>,
      ResultTypeQueryable<KEY> {
    
    @Override
    public ComparablePair<LABEL, KEY> getKey(
        StampedWindowElement<?, ? extends Pair<KEY, ?>> value) {

      return (ComparablePair)
          ComparablePair.of(value.getWindow(), value.get().getKey());
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<KEY> getProducedType() {
      return TypeInformation.of((Class) Comparable.class);
    }
  }

  static class RBKReducer
        implements ReduceFunction<StampedWindowElement<?, Pair>>,
        ResultTypeQueryable<StampedWindowElement<?, Pair>> {

    final UnaryFunction<Iterable, Object> reducer;

    RBKReducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
    }

    @Override
    public StampedWindowElement<?, Pair>
    reduce(StampedWindowElement<?, Pair> p1, StampedWindowElement<?, Pair> p2) {

      Window wid = p1.getWindow();
      return new StampedWindowElement<>(
          wid,
          Pair.of(
              p1.get().getKey(),
              reducer.apply(Arrays.asList(p1.get().getSecond(), p2.get().getSecond()))),
          Math.max(p1.getTimestamp(), p2.getTimestamp()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<StampedWindowElement<?, Pair>> getProducedType() {
      return TypeInformation.of((Class) StampedWindowElement.class);
    }
  }
}
