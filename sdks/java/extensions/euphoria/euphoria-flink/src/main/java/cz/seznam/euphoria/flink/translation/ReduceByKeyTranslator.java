package cz.seznam.euphoria.flink.translation;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.translation.functions.PartitionerWrapper;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Arrays;

class ReduceByKeyTranslator implements OperatorTranslator<ReduceByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<ReduceByKey> operator,
                                 ExecutorContext context)
  {
    ReduceByKey origOperator = operator.getOriginalOperator();
    DataStream<?> input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

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
    DataStream<Pair> tuples = (DataStream) input.map(el ->
            Pair.of(keyExtractor.apply(el), valueExtractor.apply(el)))
            .name(operator.getName() + "::map-input")
            // FIXME parallelism should be set to the same level as parent
            // since this "map-input" transformation is applied before shuffle
            .setParallelism(operator.getParallelism())
            .returns((Class) Pair.class);

    // FIXME reduce without implemented windowing will emit accumulated
    // value per each input element

    // FIXME non-combinable reduce function not supported without windowing
    if (!origOperator.isCombinable()) {
      throw new UnsupportedOperationException("Non-combinable reduce not supported yet");
    }

    // group by key + reduce
    tuples = tuples.keyBy(new TypedKeySelector())
            .reduce(new TypedReducer<>(reducer))
            .name(operator.getName())
            .setParallelism(operator.getParallelism());

    // FIXME partitioner should be applied during "reduce" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "keyBy" transformation

    // apply custom partitioner if different from default HashPartitioner
    if (!(origOperator.getPartitioning().getPartitioner().getClass() == HashPartitioner.class)) {
      tuples = tuples.partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()),
              p -> p.getKey());
    }

    return tuples;
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
      return TypeInformation.of((Class) Object.class);
    }
  }

  private static class TypedReducer<KEY>
          implements ReduceFunction<Pair<KEY, Object>>,
          ResultTypeQueryable<Pair<KEY, Object>>
  {
    final UnaryFunction<Iterable, Object> reducer;

    public TypedReducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
    }

    @Override
    public Pair<KEY, Object> reduce(Pair<KEY, Object> p1, Pair<KEY, Object> p2) {
      return Pair.of(p1.getKey(),
              reducer.apply(Arrays.asList(p1.getSecond(), p2.getSecond())));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<Pair<KEY, Object>> getProducedType() {
      return TypeInformation.of((Class) Pair.class);
    }
  }
}
