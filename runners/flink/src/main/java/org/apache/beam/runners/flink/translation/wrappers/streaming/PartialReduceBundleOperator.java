package org.apache.beam.runners.flink.translation.wrappers.streaming;

import org.apache.beam.runners.flink.translation.functions.AbstractFlinkCombineRunner;
import org.apache.beam.runners.flink.translation.functions.HashingFlinkCombineRunner;
import org.apache.beam.runners.flink.translation.functions.SortingFlinkCombineRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.flink.util.Collector;

import java.util.*;

public class PartialReduceBundleOperator<K, InputT, OutputT, AccumT> extends DoFnOperator<KV<K, InputT>, KV<K, InputT>, KV<K, AccumT>> {

  private final CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT> combineFn;

  // TODO: persist state
  private Multimap<K, WindowedValue<KV<K, InputT>>> state;

  public PartialReduceBundleOperator(
      CombineFnBase.GlobalCombineFn<InputT, AccumT, OutputT> combineFn,
      String stepName,
      Coder<WindowedValue<KV<K, InputT>>> windowedInputCoder,
      TupleTag<KV<K, AccumT>> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<KV<K, AccumT>> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options) {
    super(
        null,
        stepName,
        windowedInputCoder,
        Collections.emptyMap(),
        mainOutputTag,
        additionalOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options,
        null,
        null,
        DoFnSchemaInformation.create(),
        Collections.emptyMap());

    this.combineFn = combineFn;
    this.state = ArrayListMultimap.create();
  }

  @Override
  public void open() throws Exception {
//    clearState();
    setBundleFinishedCallback(this::finishBundle);
    super.open();
  }


  private void finishBundle() {
    AbstractFlinkCombineRunner<K, InputT, AccumT, AccumT, BoundedWindow> reduceRunner;
    try {
      if (windowingStrategy.needsMerge() && windowingStrategy.getWindowFn() instanceof Sessions) {
        reduceRunner = new SortingFlinkCombineRunner<>();
      } else {
        reduceRunner = new HashingFlinkCombineRunner<>();
      }

      for(Map.Entry<K, Collection<WindowedValue<KV<K, InputT>>>> e : state.asMap().entrySet()) {
        reduceRunner.combine(
            new AbstractFlinkCombineRunner.PartialFlinkCombiner<>(combineFn),
            (WindowingStrategy<Object, BoundedWindow>) windowingStrategy,
            sideInputReader,
            serializedOptions.get(),
            e.getValue(),
            new Collector<WindowedValue<KV<K, AccumT>>>() {
              @Override
              public void collect(WindowedValue<KV<K, AccumT>> record) {
                outputManager.output(mainOutputTag, record);
              }

              @Override
              public void close() {
              }
            });
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    clearState();
  }

  private void clearState() {
    this.state = ArrayListMultimap.create();
  }

  @Override
  protected DoFn<KV<K, InputT>, KV<K, AccumT>> getDoFn() {
    return new DoFn<KV<K, InputT>, KV<K, AccumT>>() {
      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
        WindowedValue<KV<K, InputT>> windowedValue = WindowedValue.of(c.element(), c.timestamp(), window, c.pane());
        state.put(c.element().getKey(), windowedValue);
      }
    };
  }
}
