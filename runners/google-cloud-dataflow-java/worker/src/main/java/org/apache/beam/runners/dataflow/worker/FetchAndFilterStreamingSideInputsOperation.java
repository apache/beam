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
package org.apache.beam.runners.dataflow.worker;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReceivingOperation;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This {@link ReceivingOperation} is responsible for fetching any ready side inputs and also
 * filtering any input elements that aren't ready by pushing them back into state.
 */
public class FetchAndFilterStreamingSideInputsOperation<T, W extends BoundedWindow>
    extends ReceivingOperation {

  private final StreamingSideInputFetcher<T, W> sideInputFetcher;
  private final BlockingQueue<Object> elementsToProcess;
  private final ExecutorService singleThreadExecutor;
  private Future<Void> elementProcessor;
  private static final Object POISON_PILL = new Object();

  public FetchAndFilterStreamingSideInputsOperation(
      OutputReceiver[] receivers,
      DataflowOperationContext context,
      InstructionRequestHandler instructionRequestHandler,
      FnDataService beamFnDataService,
      ApiServiceDescriptor dataServiceApiServiceDescriptor,
      IdGenerator idGenerator,
      Coder<WindowedValue<T>> inputCoder,
      WindowingStrategy<?, W> windowingStrategy,
      DataflowExecutionContext.DataflowStepContext stepContext,
      Map<PCollectionView<?>, RunnerApi.FunctionSpec> pCollectionViewToWindowMappingFns) {
    super(receivers, context);

    this.sideInputFetcher =
        new StreamingSideInputFetcher<>(
            buildPCollectionViewsWithSdkSupportedWindowMappingFn(
                idGenerator,
                instructionRequestHandler,
                beamFnDataService,
                dataServiceApiServiceDescriptor,
                ((FullWindowedValueCoder) inputCoder).getWindowCoder(),
                pCollectionViewToWindowMappingFns),
            ((WindowedValueCoder) inputCoder).getValueCoder(),
            windowingStrategy,
            (StreamingModeExecutionContext.StreamingModeStepContext)
                stepContext.namespacedToUser());
    this.elementsToProcess = new LinkedBlockingQueue<>();
    this.singleThreadExecutor = Executors.newSingleThreadExecutor();
  }

  /** A {@link PCollectionView} which forwards all calls to its delegate. */
  private static class ForwardingPCollectionView<T> implements PCollectionView<T> {
    private final PCollectionView<T> delegate;

    private ForwardingPCollectionView(PCollectionView<T> delegate) {
      this.delegate = delegate;
    }

    @Nullable
    @Override
    public PCollection<?> getPCollection() {
      return delegate.getPCollection();
    }

    @Override
    public TupleTag<?> getTagInternal() {
      return delegate.getTagInternal();
    }

    @Override
    public ViewFn<?, T> getViewFn() {
      return delegate.getViewFn();
    }

    @Override
    public WindowMappingFn<?> getWindowMappingFn() {
      return delegate.getWindowMappingFn();
    }

    @Override
    public WindowingStrategy<?, ?> getWindowingStrategyInternal() {
      return delegate.getWindowingStrategyInternal();
    }

    @Override
    public Coder<?> getCoderInternal() {
      return delegate.getCoderInternal();
    }

    @Override
    public String getName() {
      return delegate.getName();
    }

    @Override
    public Pipeline getPipeline() {
      return delegate.getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return delegate.expand();
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {
      delegate.finishSpecifyingOutput(transformName, input, transform);
    }

    @Override
    public void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform) {
      delegate.finishSpecifying(upstreamInput, upstreamTransform);
    }
  }

  private Iterable<PCollectionView<?>> buildPCollectionViewsWithSdkSupportedWindowMappingFn(
      IdGenerator idGenerator,
      InstructionRequestHandler instructionRequestHandler,
      FnDataService beamFnDataService,
      ApiServiceDescriptor dataServiceApiServiceDescriptor,
      Coder<BoundedWindow> mainInputWindowCoder,
      Map<PCollectionView<?>, RunnerApi.FunctionSpec> pCollectionViewsToWindowMappingFns) {
    ImmutableList.Builder<PCollectionView<?>> wrappedViews = ImmutableList.builder();
    for (Map.Entry<PCollectionView<?>, RunnerApi.FunctionSpec> entry :
        pCollectionViewsToWindowMappingFns.entrySet()) {
      WindowMappingFn windowMappingFn =
          new FnApiWindowMappingFn(
              idGenerator,
              instructionRequestHandler,
              dataServiceApiServiceDescriptor,
              beamFnDataService,
              entry.getValue(),
              mainInputWindowCoder,
              entry.getKey().getWindowingStrategyInternal().getWindowFn().windowCoder());
      wrappedViews.add(
          new ForwardingPCollectionView<Materializations.MultimapView>(
              (PCollectionView) entry.getKey()) {
            @Override
            public WindowMappingFn<?> getWindowMappingFn() {
              return windowMappingFn;
            }
          });
    }
    return wrappedViews.build();
  }

  @Override
  public void start() throws Exception {
    try (Closeable scope = context.enterStart()) {
      super.start();

      elementProcessor =
          singleThreadExecutor.submit(
              () -> {
                Object compressedElem;
                while ((compressedElem = elementsToProcess.take()) != POISON_PILL) {
                  for (WindowedValue<T> elem :
                      ((WindowedValue<T>) compressedElem).explodeWindows()) {
                    if (!sideInputFetcher.storeIfBlocked(elem)) {
                      outputToReceivers(elem);
                    }
                  }
                }
                return null;
              });

      // Find the set of ready windows.
      Set<W> readyWindows = sideInputFetcher.getReadyWindows();

      Iterable<BagState<WindowedValue<T>>> elementsBags =
          sideInputFetcher.prefetchElements(readyWindows);

      // Output any elements that have all their side inputs ready.
      for (BagState<WindowedValue<T>> elementsBag : elementsBags) {
        Iterable<WindowedValue<T>> elements = elementsBag.read();
        for (WindowedValue<T> elem : elements) {
          outputToReceivers(elem);
        }
        elementsBag.clear();
      }
      sideInputFetcher.releaseBlockedWindows(readyWindows);
    }
  }

  @Override
  public void process(Object compressedElement) throws Exception {
    elementsToProcess.add(compressedElement);
  }

  @Override
  public void finish() throws Exception {
    try (Closeable scope = context.enterFinish()) {
      // Add the poison pill so that the working thread finishes.
      elementsToProcess.add(POISON_PILL);
      elementProcessor.get();

      // Remove all processed elements and add all blocked elements.
      sideInputFetcher.persist();

      super.finish();
    }
  }

  @Override
  public void abort() throws Exception {
    try (Closeable scope = context.enterAbort()) {
      super.abort();
      // Cancel the element processor
      elementProcessor.cancel(true);
    }
  }

  public void outputToReceivers(WindowedValue<T> elem) throws Exception {
    for (OutputReceiver receiver : receivers) {
      receiver.process(elem);
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
