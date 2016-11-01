package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
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
import cz.seznam.euphoria.flink.functions.IteratorIterable;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterators;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.util.Collector;

public class ReduceByKeyTranslator implements BatchOperatorTranslator<ReduceByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public DataSet translate(FlinkOperator<ReduceByKey> operator,
                           BatchExecutorContext context) {
    
    DataSet<?> input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceByKey origOperator = operator.getOriginalOperator();
    final UnaryFunction<Iterable, Object> reducer = origOperator.getReducer();
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

    // extract key/value from data
    DataSet<WindowedElement> tuples = (DataSet) input.flatMap((i, c) -> {
          WindowedElement wel = (WindowedElement) i;
          Set<Window> assigned = windowing.assignWindowsToElement(wel);
          for (Window wid : assigned) {
            Object el = wel.get();
            c.collect(new WindowedElement(
                wid, Pair.of(udfKey.apply(el), udfValue.apply(el))));
          }
        })
        .name(operator.getName() + "::map-input")
        // FIXME parallelism should be set to the same level as parent
        // since this "map-input" transformation is applied before shuffle
        .setParallelism(operator.getParallelism())
        .returns((Class) WindowedElement.class);


    // FIXME: missing window merging here

    // XXX require keyExtractor to deliver `Comparable`s

    Operator<WindowedElement<?, Pair>, ?> reduced;
    if (origOperator.isCombinable()) {
      reduced = tuples
          .groupBy((KeySelector) new TypedKeySelector<>())
          .reduce(new TypedReducer(reducer));
    } else {
      reduced = tuples
          .groupBy((KeySelector) new TypedKeySelector<>())
          .reduceGroup(new TypedGroupReducer(reducer));
    }
    reduced = reduced
        .setParallelism(operator.getParallelism())
        .name(operator.getName() + "::reduce");

    // FIXME partitioner should be applied during "reduce" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "groupBy" transformation

    // apply custom partitioner if different from default HashPartitioner
    if (!(origOperator.getPartitioning().getPartitioner().getClass() == HashPartitioner.class)) {
      reduced = reduced
          .partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()),
              Utils.wrapQueryable(
                  (WindowedElement<?, Pair> we) -> (Comparable) we.get().getKey(),
                  Comparable.class))
          .setParallelism(operator.getParallelism());
    }

    return reduced;
  }

  @SuppressWarnings("unchecked")
  private static class TypedKeySelector<LABEL, KEY>
      implements KeySelector<WindowedElement<?, ? extends Pair<KEY, ?>>,
          ComparablePair<LABEL, KEY>>,
      ResultTypeQueryable<KEY> {
    
    @Override
    public ComparablePair<LABEL, KEY> getKey(
        WindowedElement<?, ? extends Pair<KEY, ?>> value) {
      
      return (ComparablePair) ComparablePair.of(
          value.getWindow(), value.get().getKey());
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<KEY> getProducedType() {
      return TypeInformation.of((Class) Comparable.class);
    }
    
  }

  private static class TypedReducer
          implements ReduceFunction<WindowedElement<?, Pair>>,
          ResultTypeQueryable<WindowedElement<?, Pair>>
  {
    final UnaryFunction<Iterable, Object> reducer;

    public TypedReducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
    }

    @Override
    public WindowedElement<?, Pair>
    reduce(WindowedElement<?, Pair> p1, WindowedElement<?, Pair> p2) {
      Window wid = p1.getWindow();
      return new WindowedElement<>(wid,
        Pair.of(
            p1.get().getKey(),
            reducer.apply(Arrays.asList(p1.get().getSecond(), p2.get().getSecond()))));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<WindowedElement<?, Pair>> getProducedType() {
      return TypeInformation.of((Class) WindowedElement.class);
    }
  } // ~ end of TypedReducer

  private static class TypedGroupReducer
          implements GroupReduceFunction<WindowedElement<?, Pair>, WindowedElement<?, Pair>>,
          ResultTypeQueryable<WindowedElement<?, Pair>>
  {
    final UnaryFunction<Iterable, Object> reducer;

    public TypedGroupReducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
    }

    @Override
    public void reduce(Iterable<WindowedElement<?, Pair>> input,
                       Collector<WindowedElement<?, Pair>> collector)
        throws Exception {
      Iterator<WindowedElement<?, Pair>> inputIter = input.iterator();
      // ~ get the first element (there is always at least one) to obtain window metadata
      WindowedElement<?, Pair> first = inputIter.next();
      // ~ re-construct the input unwrapped out of the window-element-envelopes
      // for passing it to the user supplied reduce function
      IteratorIterable<Object> unwrapped =
          new IteratorIterable<>(Iterators.transform(
              Iterators.concat(Iterators.singletonIterator(first), inputIter),
              e -> e.get().getSecond()));
      Object out = reducer.apply(unwrapped);
      // ~ produce output
      collector.collect(new WindowedElement<>(
          first.getWindow(), Pair.of(first.get().getFirst(), out)));
    }

    @Override
    public TypeInformation<WindowedElement<?, Pair>> getProducedType() {
      return TypeInformation.of((Class) WindowedElement.class);
    }
  } // ~ end of TypedGroupReducer
}
