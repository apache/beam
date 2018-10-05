package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindow;
import cz.seznam.euphoria.flink.streaming.windowing.MultiWindowedElement;
import cz.seznam.euphoria.flink.streaming.windowing.WindowProperties;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;

import java.util.Iterator;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

class ReduceStateByKeyTranslator implements StreamingOperatorTranslator<ReduceStateByKey> {

  FlinkStreamingStateStorageProvider storageProvider
      = new FlinkStreamingStateStorageProvider();

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<ReduceStateByKey> operator,
                                 StreamingExecutorContext context)
  {
    DataStream<?> input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceStateByKey origOperator = operator.getOriginalOperator();

    StateFactory<?, State> stateFactory = origOperator.getStateFactory();

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

    DataStream<StreamingWindowedElement<?, ?, WindowedPair>> folded;
    // apply windowing first
    if (windowing == null) {
      WindowedStream windowedPairs =
          context.attachedWindowStream((DataStream) input, keyExtractor, valueExtractor);
      // equivalent operation to "left fold"
      folded = windowedPairs.apply(new RSBKWindowFunction(storageProvider, stateFactory))
          .name(operator.getName())
          .setParallelism(operator.getParallelism());
    } else {
      // FIXME merging windows not covered yet
      WindowedStream<MultiWindowedElement<?, ?, Pair>, Object, FlinkWindow>
          windowedPairs = context.flinkWindow((DataStream) input, keyExtractor, valueExtractor, windowing);
      // equivalent operation to "left fold"
      folded = windowedPairs.apply(new RSBKWindowFunction(storageProvider, stateFactory))
          .name(operator.getName())
          .setParallelism(operator.getParallelism());
    }

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

  private static class RSBKWindowFunction<
      GROUP, LABEL, KEY, VALUEIN, VALUEOUT,
      W extends Window & WindowProperties<GROUP, LABEL>>
      extends RichWindowFunction<ElementProvider<? extends Pair<KEY, VALUEIN>>,
      StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUEOUT>>,
      KEY,
      W> {

    private final StateFactory<?, State> stateFactory;
    private final FlinkStreamingStateStorageProvider storageProvider;

    RSBKWindowFunction(
        FlinkStreamingStateStorageProvider storageProvider,
        StateFactory<?, State> stateFactory) {

      this.stateFactory = stateFactory;
      this.storageProvider = storageProvider;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      storageProvider.initialize(getRuntimeContext());
    }

    @Override
    public void apply(
        KEY key,
        W window,
        Iterable<ElementProvider<? extends Pair<KEY, VALUEIN>>> input,
        Collector<StreamingWindowedElement<GROUP, LABEL, WindowedPair<LABEL, KEY, VALUEOUT>>> out)
        throws Exception {

      Iterator<ElementProvider<? extends Pair<KEY, VALUEIN>>> it = input.iterator();
      // read the first element to obtain window metadata and key
      ElementProvider<? extends Pair<KEY, VALUEIN>> element = it.next();
      WindowID wid = window.getWindowID();
      long emissionWatermark = window.getEmissionWatermark();

      State state = stateFactory.apply(
          e -> out.collect(new StreamingWindowedElement<>(
              wid,
              WindowedPair.of(
                  wid.getLabel(),
                  key,
                  e))
              // ~ forward the emission watermark
              .withEmissionWatermark(emissionWatermark)), storageProvider);

      // add the first element to the state
      state.add(element.get().getValue());

      while (it.hasNext()) {
        state.add(it.next().get().getValue());
      }
      state.flush();
      state.close();
    }
  }
}
