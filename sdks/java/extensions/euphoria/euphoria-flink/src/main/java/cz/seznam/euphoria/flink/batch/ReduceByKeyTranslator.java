package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.util.Arrays;

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
    final UnaryFunction keyExtractor;
    final UnaryFunction valueExtractor;

    if (origOperator.isGrouped()) {
      UnaryFunction reduceKeyExtractor = origOperator.getKeyExtractor();
      keyExtractor = (UnaryFunction<Pair, CompositeKey>)
              (Pair p) -> CompositeKey.of(
                      p.getFirst(),
                      reduceKeyExtractor.apply(p.getSecond()));
      UnaryFunction vfn = origOperator.getValueExtractor();
      valueExtractor = (UnaryFunction<Pair, Object>)
              (Pair p) -> vfn.apply(p.getSecond());
    } else {
      keyExtractor = origOperator.getKeyExtractor();
      valueExtractor = origOperator.getValueExtractor();
    }

    // extract key/value from data
    DataSet<Pair> tuples = (DataSet) input.map(el ->
            Pair.of(keyExtractor.apply(el), valueExtractor.apply(el)))
            .name(operator.getName() + "::map-input")
            // FIXME parallelism should be set to the same level as parent
            // since this "map-input" transformation is applied before shuffle
            .setParallelism(operator.getParallelism())
            .returns((Class) Pair.class);

    // XXX require keyExtractor to deliver `Comparable`s
    return tuples.groupBy((KeySelector) new TypedKeySelector<>())
        .reduce(new TypedReducer(reducer))
        .setParallelism(operator.getParallelism())
        .name(operator.getName());
  }

  private static class TypedKeySelector<KEY>
      implements KeySelector<Pair<KEY, Object>, KEY>, ResultTypeQueryable<KEY>
  {
    @Override
    public KEY getKey(Pair<KEY, Object> pair) throws Exception {
      return pair.getKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<KEY> getProducedType() {
      return TypeInformation.of((Class) Comparable.class);
    }
  }

  private static class TypedReducer
          implements ReduceFunction<Pair>,
          ResultTypeQueryable<Pair>
  {
    final UnaryFunction<Iterable, Object> reducer;

    public TypedReducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
    }

    @Override
    public Pair reduce(Pair p1, Pair p2) {
      return Pair.of(p1.getKey(),
              reducer.apply(Arrays.asList(p1.getSecond(), p2.getSecond())));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<Pair> getProducedType() {
      return TypeInformation.of((Class) Pair.class);
    }
  }
}
