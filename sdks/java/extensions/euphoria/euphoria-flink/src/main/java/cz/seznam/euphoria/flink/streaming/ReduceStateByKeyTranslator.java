package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.State;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Iterator;

class ReduceStateByKeyTranslator implements StreamingOperatorTranslator<ReduceStateByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<ReduceStateByKey> operator,
                                 StreamingExecutorContext context)
  {
    DataStream<?> input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceStateByKey origOperator = operator.getOriginalOperator();

    UnaryFunction<Collector<?>, State> stateFactory = origOperator.getStateFactory();

    final UnaryFunction keyExtractor;
    final UnaryFunction valueExtractor;
    final Windowing windowing = origOperator.getWindowing();

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

    // apply windowing first
    WindowedStream<WindowedElement<?, ?, WindowedPair>, Object, Window> windowedPairs =
            context.windowStream((DataStream) input, keyExtractor, valueExtractor, windowing);

    // equivalent operation to "left fold"
    DataStream<WindowedElement<?, ?, WindowedPair>> folded = windowedPairs.apply(
            new WindowFolder(stateFactory))
            .name(operator.getName())
            .setParallelism(operator.getParallelism());

    // FIXME partitioner should be applied during "keyBy" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "keyBy" transformation

    // apply custom partitioner if different from default HashPartitioner
    if (!(origOperator.getPartitioning().getPartitioner().getClass() == HashPartitioner.class)) {
      folded = folded.partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()),
              p -> p.get().getKey());
    }

    return folded;
  }

  private static class WindowFolder
          implements WindowFunction<WindowedElement<?, ?, WindowedPair>, WindowedElement<?, ?, WindowedPair>, Object, Window> {

    private final UnaryFunction<Collector<?>, State> stateFactory;

    public WindowFolder(UnaryFunction<Collector<?>, State> stateFactory) {
      this.stateFactory = stateFactory;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(Object o,
                      Window window,
                      Iterable<WindowedElement<?, ?, WindowedPair>> input,
                      org.apache.flink.util.Collector<WindowedElement<?, ?, WindowedPair>> out)
    {
      Iterator<WindowedElement<?, ?, WindowedPair>> it = input.iterator();

      // read the first element to obtain window metadata and key
      WindowedElement<?, ?, WindowedPair> element = it.next();
      WindowID wid = element.getWindowID();
      Object key = element.get().getKey();

      State state = stateFactory.apply(e -> out.collect(new WindowedElement<>(
              wid,
              WindowedPair.of(
                      wid.getLabel(),
                      key,
                      e))));

      // add the first element to the state
      state.add(element.get().getValue());

      while (it.hasNext()) {
        state.add(it.next().get().getValue());
      }
      state.flush();
    }
  }
}
