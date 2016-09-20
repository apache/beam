package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.Arrays;
import org.apache.flink.api.java.operators.Operator;

public class ReduceByKeyTranslator implements BatchOperatorTranslator<ReduceByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public DataSet translate(FlinkOperator<ReduceByKey> operator,
                           BatchExecutorContext context)
  {
    DataSet<?> input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceByKey origOperator = operator.getOriginalOperator();
    final UnaryFunction<Iterable, Object> reducer = origOperator.getReducer();

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
    DataSet<WindowedElement> tuples = (DataSet) input.map(i -> {
          WindowedElement wel = (WindowedElement) i;
          WindowID wid = wel.getWindowID();
          Object el = wel.get();
          return new WindowedElement(
              wid, WindowedPair.of(wid.getLabel(), udfKey.apply(el), udfValue.apply(el)));
        })
        .name(operator.getName() + "::map-input")
        // FIXME parallelism should be set to the same level as parent
        // since this "map-input" transformation is applied before shuffle
        .setParallelism(operator.getParallelism())
        .returns((Class) WindowedElement.class);



    // XXX require keyExtractor to deliver `Comparable`s
    Operator<WindowedElement<?, ?, Pair>, ?> reduced = tuples
        .groupBy((KeySelector) new TypedKeySelector<>())
        .reduce(new TypedReducer(reducer))
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
              new TypedKeySelector())
          .setParallelism(operator.getParallelism());
    }

    return reduced;
  }

  private static class TypedKeySelector<KEY>
      implements KeySelector<WindowedElement<?, ?, ? extends Pair<KEY, ?>>, KEY>,
      ResultTypeQueryable<KEY>
  {
    @Override
    public KEY getKey(WindowedElement<?, ?, ? extends Pair<KEY, ?>> value)
        throws Exception
    {
      return value.get().getKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<KEY> getProducedType() {
      return TypeInformation.of((Class) Comparable.class);
    }
  }

  private static class TypedReducer
          implements ReduceFunction<WindowedElement<?, ?, Pair>>,
          ResultTypeQueryable<WindowedElement<?, ?, Pair>>
  {
    final UnaryFunction<Iterable, Object> reducer;

    public TypedReducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
    }

    @Override
    public WindowedElement<?, ?, Pair>
    reduce(WindowedElement<?, ?, Pair> p1, WindowedElement<?, ?, Pair> p2) {
      WindowID<?, ?> wid = p1.getWindowID();
      return new WindowedElement<>(wid,
        WindowedPair.of(wid.getLabel(), p1.get().getKey(),
          reducer.apply(Arrays.asList(p1.get().getSecond(), p2.get().getSecond()))));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<WindowedElement<?, ?, Pair>> getProducedType() {
      return TypeInformation.of((Class) WindowedElement.class);
    }
  }
}
