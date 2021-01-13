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
package org.apache.beam.runners.jet.processors;

import com.hazelcast.jet.core.Processor;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's ParDo primitive (when no
 * user-state is being used).
 */
public class ParDoP<InputT, OutputT>
    extends AbstractParDoP<InputT, OutputT> { // todo: unify with StatefulParDoP?

  private ParDoP(
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<TupleTag<?>, int[]> outputCollToOrdinals,
      SerializablePipelineOptions pipelineOptions,
      TupleTag<OutputT> mainOutputTag,
      Coder<InputT> inputCoder,
      Map<PCollectionView<?>, Coder<?>> sideInputCoders,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      Coder<InputT> inputValueCoder,
      Map<TupleTag<?>, Coder<?>> outputValueCoders,
      Map<Integer, PCollectionView<?>> ordinalToSideInput,
      Map<String, PCollectionView<?>> sideInputMapping,
      String ownerId,
      String stepId) {
    super(
        doFn,
        windowingStrategy,
        doFnSchemaInformation,
        outputCollToOrdinals,
        pipelineOptions,
        mainOutputTag,
        inputCoder,
        sideInputCoders,
        outputCoders,
        inputValueCoder,
        outputValueCoders,
        ordinalToSideInput,
        sideInputMapping,
        ownerId,
        stepId);
  }

  @Override
  protected DoFnRunner<InputT, OutputT> getDoFnRunner(
      PipelineOptions pipelineOptions,
      DoFn<InputT, OutputT> doFn,
      SideInputReader sideInputReader,
      JetOutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      Coder<InputT> inputValueCoder,
      Map<TupleTag<?>, Coder<?>> outputValueCoders,
      WindowingStrategy<?, ?> windowingStrategy,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {
    return DoFnRunners.simpleRunner(
        pipelineOptions,
        doFn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        new NotImplementedStepContext(),
        inputValueCoder,
        outputValueCoders,
        windowingStrategy,
        doFnSchemaInformation,
        sideInputMapping);
  }

  /**
   * Jet {@link Processor} supplier that will provide instances of {@link ParDoP}.
   *
   * @param <OutputT> the type of main output elements of the DoFn being used
   */
  public static class Supplier<InputT, OutputT> extends AbstractSupplier<InputT, OutputT> {

    public Supplier(
        String stepId,
        String ownerId,
        DoFn<InputT, OutputT> doFn,
        WindowingStrategy<?, ?> windowingStrategy,
        DoFnSchemaInformation doFnSchemaInformation,
        SerializablePipelineOptions pipelineOptions,
        TupleTag<OutputT> mainOutputTag,
        Set<TupleTag<OutputT>> allOutputTags,
        Coder<InputT> inputCoder,
        Map<PCollectionView<?>, Coder<?>> sideInputCoders,
        Map<TupleTag<?>, Coder<?>> outputCoders,
        Coder<InputT> inputValueCoder,
        Map<TupleTag<?>, Coder<?>> outputValueCoders,
        Collection<PCollectionView<?>> sideInputs,
        Map<String, PCollectionView<?>> sideInputMapping) {
      super(
          stepId,
          ownerId,
          doFn,
          windowingStrategy,
          doFnSchemaInformation,
          pipelineOptions,
          mainOutputTag,
          allOutputTags,
          inputCoder,
          sideInputCoders,
          outputCoders,
          inputValueCoder,
          outputValueCoders,
          sideInputs,
          sideInputMapping);
    }

    @Override
    Processor getEx(
        DoFn<InputT, OutputT> doFn,
        WindowingStrategy<?, ?> windowingStrategy,
        DoFnSchemaInformation doFnSchemaInformation,
        Map<TupleTag<?>, int[]> outputCollToOrdinals,
        SerializablePipelineOptions pipelineOptions,
        TupleTag<OutputT> mainOutputTag,
        Coder<InputT> inputCoder,
        Map<PCollectionView<?>, Coder<?>> sideInputCoders,
        Map<TupleTag<?>, Coder<?>> outputCoders,
        Coder<InputT> inputValueCoder,
        Map<TupleTag<?>, Coder<?>> outputValueCoders,
        Map<Integer, PCollectionView<?>> ordinalToSideInput,
        Map<String, PCollectionView<?>> sideInputMapping,
        String ownerId,
        String stepId) {
      return new ParDoP<>(
          doFn,
          windowingStrategy,
          doFnSchemaInformation,
          outputCollToOrdinals,
          pipelineOptions,
          mainOutputTag,
          inputCoder,
          sideInputCoders,
          outputCoders,
          inputValueCoder,
          outputValueCoders,
          ordinalToSideInput,
          sideInputMapping,
          ownerId,
          stepId);
    }
  }

  private static class NotImplementedStepContext implements StepContext {

    // not needed when not handling state & timers

    @Override
    public StateInternals stateInternals() {
      throw new UnsupportedOperationException("stateInternals is not supported");
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException("timerInternals is not supported");
    }
  }
}
