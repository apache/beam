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

import static org.apache.flink.util.Preconditions.checkState;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageContext;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * ExecutableStageDoFnOperator basic functional implementation without side inputs and user state.
 * SDK harness interaction code adopted from
 * {@link org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageFunction}.
 * TODO: Evaluate reuse
 * All operators in the non-portable streaming translation are based on {@link DoFnOperator}.
 * This implies dependency on {@link DoFnRunner}, which is not required for portable pipeline.
 * TODO: Multiple element bundle execution
 * The operator (like old non-portable runner) executes every element as separate bundle,
 * which will be even more expensive with SDK harness container.
 * Refactor for above should be looked into once streaming side inputs (and push back) take
 * shape.
 */
public class ExecutableStageDoFnOperator<InputT, OutputT> extends DoFnOperator<InputT, OutputT> {

  private static final Logger logger =
          Logger.getLogger(ExecutableStageDoFnOperator.class.getName());

  private final RunnerApi.ExecutableStagePayload payload;
  private final JobInfo jobInfo;
  private final FlinkExecutableStageContext.Factory contextFactory;
  private final Map<String, TupleTag<?>> outputMap;

  private transient FlinkExecutableStageContext stageContext;
  private transient StateRequestHandler stateRequestHandler;
  private transient BundleProgressHandler progressHandler;
  private transient StageBundleFactory stageBundleFactory;

  public ExecutableStageDoFnOperator(String stepName,
                                     Coder<WindowedValue<InputT>> inputCoder,
                                     TupleTag<OutputT> mainOutputTag,
                                     List<TupleTag<?>> additionalOutputTags,
                                     OutputManagerFactory<OutputT> outputManagerFactory,
                                     Map<Integer, PCollectionView<?>> sideInputTagMapping,
                                     Collection<PCollectionView<?>> sideInputs,
                                     PipelineOptions options,
                                     RunnerApi.ExecutableStagePayload payload,
                                     JobInfo jobInfo,
                                     FlinkExecutableStageContext.Factory contextFactory
                                     ) {
    super(new NoOpDoFn(),
            stepName, inputCoder, mainOutputTag, additionalOutputTags,
            outputManagerFactory, WindowingStrategy.globalDefault() /* unused */,
            sideInputTagMapping, sideInputs, options, null /*keyCoder*/, null /* key selector */);
      this.payload = payload;
      this.jobInfo = jobInfo;
      this.contextFactory = contextFactory;
      this.outputMap = createOutputMap(mainOutputTag, additionalOutputTags);
  }

  private static Map<String, TupleTag<?>> createOutputMap(TupleTag mainOutput,
                                                          List<TupleTag<?>> additionalOutputs) {
      Map<String, TupleTag<?>> outputMap = new HashMap<>(additionalOutputs.size() + 1);
      if (mainOutput != null) {
        outputMap.put(mainOutput.getId(), mainOutput);
      }
      for (TupleTag<?> additionalTag : additionalOutputs) {
        outputMap.put(additionalTag.getId(), additionalTag);
      }
      return outputMap;
  }

  @Override
  public void open() throws Exception {
    super.open();

    ExecutableStage executableStage = ExecutableStage.fromPayload(payload);
    // TODO: Wire this into the distributed cache and make it pluggable.
    // TODO: Do we really want this layer of indirection when accessing the stage bundle factory?
    // It's a little strange because this operator is responsible for the lifetime of the stage
    // bundle "factory" (manager?) but not the job or Flink bundle factories. How do we make
    // ownership of the higher level "factories" explicit? Do we care?
    stageContext = contextFactory.get(jobInfo);
    // NOTE: It's safe to reuse the state handler between partitions because each partition uses the
    // same backing runtime context and broadcast variables. We use checkState below to catch errors
    // in backward-incompatible Flink changes.
    stateRequestHandler = stageContext.getStateRequestHandler(executableStage, getRuntimeContext());
    stageBundleFactory = stageContext.getStageBundleFactory(executableStage);
    progressHandler = BundleProgressHandler.unsupported();
  }

  // TODO: currently assumes that every element is a separate bundle,
  // but this can be changed by pushing some of this logic into the "DoFnRunner"
  private void processElementWithSdkHarness(WindowedValue<InputT> element) throws Exception {
    checkState(stageBundleFactory != null, "%s not yet prepared",
            StageBundleFactory.class.getName());
    checkState(stateRequestHandler != null, "%s not yet prepared",
            StateRequestHandler.class.getName());

    try (RemoteBundle<InputT> bundle =
        stageBundleFactory.getBundle(
            new ReceiverFactory(outputManager, outputMap), stateRequestHandler, progressHandler)) {
      logger.finer(String.format("Sending value: %s", element));
      bundle.getInputReceiver().accept(element);
    }
  }

  @Override
  public void close() throws Exception {
    try (AutoCloseable bundleFactoryCloser = stageBundleFactory) {}
    // Remove the reference to stageContext and make stageContext available for garbage collection.
    stageContext = null;
    super.close();
  }

  // TODO: remove single element bundle assumption
  @Override
  protected DoFnRunner<InputT, OutputT> createWrappingDoFnRunner(
          DoFnRunner<InputT, OutputT> wrappedRunner) {
    return new SdkHarnessDoFnRunner();
  }

  private class SdkHarnessDoFnRunner implements DoFnRunner<InputT, OutputT> {
    @Override
    public void startBundle() {}

    @Override
    public void processElement(WindowedValue<InputT> elem) {
      try {
        processElementWithSdkHarness(elem);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onTimer(String timerId, BoundedWindow window, Instant timestamp,
                        TimeDomain timeDomain) {
    }

    @Override
    public void finishBundle() {}

    @Override
    public DoFn<InputT, OutputT> getFn() {
      throw new UnsupportedOperationException();
    }
  }

  private static class NoOpDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
    @ProcessElement
    public void doNothing(ProcessContext context) {}
  }

  /**
   * Receiver factory that wraps outgoing elements with the corresponding union tag for a
   * multiplexed PCollection.
   */
  private static class ReceiverFactory implements OutputReceiverFactory {

    private final Object collectorLock = new Object();

    @GuardedBy("collectorLock")
    private final BufferedOutputManager<RawUnionValue> collector;

    private final Map<String, TupleTag<?>> outputMap;

    ReceiverFactory(BufferedOutputManager collector, Map<String, TupleTag<?>> outputMap) {
      this.collector = collector;
      this.outputMap = outputMap;
    }

    @Override
    public <OutputT> FnDataReceiver<OutputT> create(String collectionId) {
      return (receivedElement) -> {
        synchronized (collectorLock) {
          collector.output(outputMap.get(collectionId), (WindowedValue) receivedElement);
        }
      };
    }
  }

}
