/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.SerializableFnAggregatorWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.DoFnRunner;
import org.apache.beam.sdk.util.DoFnRunners;
import org.apache.beam.sdk.util.ExecutionContext;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Flink operator for executing {@link DoFn DoFns}.
 *
 * @param <InputT>
 * @param <FnOutputT>
 * @param <OutputT>
 */
public class DoFnOperator<InputT, FnOutputT, OutputT>
    extends AbstractStreamOperator<OutputT>
    implements OneInputStreamOperator<WindowedValue<InputT>, OutputT> {

  protected OldDoFn<InputT, FnOutputT> doFn;
  protected final SerializedPipelineOptions serializedOptions;

  protected final TupleTag<FnOutputT> mainOutputTag;
  protected final List<TupleTag<?>> sideOutputTags;

  protected final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;

  protected final boolean hasSideInputs;

  protected final WindowingStrategy<?, ?> windowingStrategy;

  protected final OutputManagerFactory<OutputT> outputManagerFactory;

  protected transient DoFnRunner<InputT, FnOutputT> doFnRunner;

  /**
   * To keep track of the current watermark so that we can immediately fire if a trigger
   * registers an event time callback for a timestamp that lies in the past.
   */
  protected transient long currentWatermark = Long.MIN_VALUE;

  public DoFnOperator(
      OldDoFn<InputT, FnOutputT> doFn,
      TupleTag<FnOutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions options) {
    this.doFn = doFn;
    this.mainOutputTag = mainOutputTag;
    this.sideOutputTags = sideOutputTags;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializedPipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
    this.outputManagerFactory = outputManagerFactory;

    this.hasSideInputs = !sideInputs.isEmpty();

    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  protected ExecutionContext.StepContext createStepContext() {
    return new StepContext();
  }

  // allow overriding this in WindowDoFnOperator because this one dynamically creates
  // the DoFn
  protected OldDoFn<InputT, FnOutputT> getDoFn() {
    return doFn;
  }

  @Override
  public void open() throws Exception {
    super.open();

    this.doFn = getDoFn();

    Aggregator.AggregatorFactory aggregatorFactory = new Aggregator.AggregatorFactory() {
      @Override
      public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
          Class<?> fnClass,
          ExecutionContext.StepContext stepContext,
          String aggregatorName,
          Combine.CombineFn<InputT, AccumT, OutputT> combine) {
        SerializableFnAggregatorWrapper<InputT, OutputT> result =
            new SerializableFnAggregatorWrapper<>(combine);

        getRuntimeContext().addAccumulator(aggregatorName, result);
        return result;
      }
    };

    doFnRunner = DoFnRunners.createDefault(
        serializedOptions.getPipelineOptions(),
        doFn,
        null,
        outputManagerFactory.create(output),
        mainOutputTag,
        sideOutputTags,
        createStepContext(),
        aggregatorFactory,
        windowingStrategy);

    doFnRunner.startBundle();
    doFn.setup();
  }

  @Override
  public void close() throws Exception {
    super.close();
    doFnRunner.finishBundle();
    doFn.teardown();
  }

  @Override
  public final void processElement(StreamRecord<WindowedValue<InputT>> streamRecord) throws Exception {
    doFnRunner.processElement(streamRecord.getValue());
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    output.emitWatermark(mark);
  }

  /**
   * Factory for creating an {@link DoFnRunners.OutputManager} from
   * a Flink {@link Output}.
   */
  interface OutputManagerFactory<OutputT> extends Serializable {
    DoFnRunners.OutputManager create(Output<StreamRecord<OutputT>> output);
  }

  /**
   * Default implementation of {@link OutputManagerFactory} that creates an
   * {@link DoFnRunners.OutputManager} that only writes to
   * a single logical output.
   */
  public static class DefaultOutputManagerFactory<OutputT>
      implements OutputManagerFactory<OutputT> {
    @Override
    public DoFnRunners.OutputManager create(final Output<StreamRecord<OutputT>> output) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> value) {
          // with side outputs we can't get around this because we don't
          // know our own output type...
          @SuppressWarnings("unchecked")
          OutputT castValue = (OutputT) value;
          output.collect(new StreamRecord<>(castValue));
        }
      };
    }
  }

  /**
   * Implementation of {@link OutputManagerFactory} that creates an
   * {@link DoFnRunners.OutputManager} that can write to multiple logical
   * outputs by unioning them in a {@link RawUnionValue}.
   */
  public static class MultiOutputOutputManagerFactory
      implements OutputManagerFactory<RawUnionValue> {

    Map<TupleTag<?>, Integer> mapping;

    public MultiOutputOutputManagerFactory(Map<TupleTag<?>, Integer> mapping) {
      this.mapping = mapping;
    }

    @Override
    public DoFnRunners.OutputManager create(final Output<StreamRecord<RawUnionValue>> output) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> value) {
          int intTag = mapping.get(tag);
          output.collect(new StreamRecord<>(new RawUnionValue(intTag, value)));
        }
      };
    }
  }

  /**
   * {@link StepContext} for running {@link DoFn DoFns} on Flink. This does not allow
   * accessing state or timer internals.
   */
  protected class StepContext implements ExecutionContext.StepContext {

    @Override
    public String getStepName() {
      return null;
    }

    @Override
    public String getTransformName() {
      return null;
    }

    @Override
    public void noteOutput(WindowedValue<?> output) {}

    @Override
    public void noteSideOutput(TupleTag<?> tag, WindowedValue<?> output) {}

    @Override
    public <T, W extends BoundedWindow> void writePCollectionViewData(
        TupleTag<?> tag,
        Iterable<WindowedValue<T>> data,
        Coder<Iterable<WindowedValue<T>>> dataCoder,
        W window,
        Coder<W> windowCoder) throws IOException {
      throw new UnsupportedOperationException("Writing side-input data is not supported.");
    }

    @Override
    public StateInternals<?> stateInternals() {
      throw new UnsupportedOperationException("Not supported for regular DoFns.");
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException("Not supported for regular DoFns.");
    }
  }

}
