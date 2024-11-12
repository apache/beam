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
package org.apache.beam.fn.harness;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.control.BundleProgressReporter;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.BeamFnDataOutboundAggregator;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.DataEndpoint;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.TimerEndpoint;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.Timer;
import org.joda.time.Instant;

/**
 * A test utility to simplify building and using a {@link PTransformRunnerFactory.Context} for
 * tests.
 */
@AutoValue
public abstract class PTransformRunnerFactoryTestContext
    implements PTransformRunnerFactory.Context {

  /** Returns a builder for the specified PTransform id and PTransform definition. */
  public static Builder builder(String pTransformId, RunnerApi.PTransform pTransform) {
    return new AutoValue_PTransformRunnerFactoryTestContext.Builder()
        .pipelineOptions(PipelineOptionsFactory.create())
        .shortIdMap(new ShortIdMap())
        .beamFnDataClient(
            new BeamFnDataClient() {
              @Override
              public void registerReceiver(
                  String instructionId,
                  List<ApiServiceDescriptor> apiServiceDescriptors,
                  CloseableFnDataReceiver<Elements> receiver) {
                throw new UnsupportedOperationException("Unexpected call during test.");
              }

              @Override
              public void unregisterReceiver(
                  String instructionId, List<ApiServiceDescriptor> apiServiceDescriptors) {
                throw new UnsupportedOperationException("Unexpected call during test.");
              }

              @Override
              public BeamFnDataOutboundAggregator createOutboundAggregator(
                  ApiServiceDescriptor apiServiceDescriptor,
                  Supplier<String> processBundleRequestIdSupplier,
                  boolean collectElementsIfNoFlushes) {
                throw new UnsupportedOperationException("Unexpected call during test.");
              }

              @Override
              public void poisonInstructionId(String instructionId) {
                throw new UnsupportedOperationException("Unexpected call during test.");
              }
            })
        .beamFnStateClient(
            new BeamFnStateClient() {
              @Override
              public CompletableFuture<StateResponse> handle(StateRequest.Builder requestBuilder) {
                throw new UnsupportedOperationException("Unexpected call during test.");
              }
            })
        .pTransformId(pTransformId)
        .pTransform(pTransform)
        .processBundleInstructionIdSupplier(
            () -> {
              throw new UnsupportedOperationException("Unexpected call during test.");
            })
        .cacheTokensSupplier(() -> Collections.emptyList())
        .bundleCacheSupplier(() -> Caches.noop())
        .processWideCache(Caches.noop())
        .pCollections(Collections.emptyMap()) // expected to be immutable
        .coders(Collections.emptyMap()) // expected to be immutable
        .windowingStrategies(Collections.emptyMap()) // expected to be immutable
        .pCollectionConsumers(new HashMap<>())
        .startBundleFunctions(new ArrayList<>())
        .finishBundleFunctions(new ArrayList<>())
        .resetFunctions(new ArrayList<>())
        .tearDownFunctions(new ArrayList<>())
        .bundleProgressReporters(new ArrayList<>())
        .incomingDataEndpoints(new HashMap<>())
        .incomingTimerEndpoints(new ArrayList<>())
        .outgoingDataEndpoints(new HashMap<>())
        .outgoingTimersEndpoints(new ArrayList<>())
        .outboundAggregators(new HashMap<>())
        .timerApiServiceDescriptor(ApiServiceDescriptor.getDefaultInstance())
        .splitListener(
            new BundleSplitListener() {
              @Override
              public void split(
                  List<BundleApplication> primaryRoots,
                  List<DelayedBundleApplication> residualRoots) {
                throw new UnsupportedOperationException("Unexpected call during test.");
              }
            })
        .bundleFinalizer(
            new BundleFinalizer() {
              @Override
              public void afterBundleCommit(Instant callbackExpiry, Callback callback) {
                throw new UnsupportedOperationException("Unexpected call during test.");
              }
            })
        .runnerCapabilities(new HashSet<>());
  }

  /** A builder to create a context for tests. */
  @AutoValue.Builder
  public interface Builder {
    Builder pipelineOptions(PipelineOptions value);

    Builder shortIdMap(ShortIdMap shortIdMap);

    Builder beamFnDataClient(BeamFnDataClient value);

    Builder beamFnStateClient(BeamFnStateClient value);

    Builder pTransformId(String value);

    Builder pTransform(RunnerApi.PTransform value);

    Builder processBundleInstructionIdSupplier(Supplier<String> value);

    Builder cacheTokensSupplier(Supplier<List<BeamFnApi.ProcessBundleRequest.CacheToken>> value);

    Builder bundleCacheSupplier(Supplier<Cache<?, ?>> value);

    Builder processWideCache(Cache<?, ?> value);

    default Builder processBundleInstructionId(String value) {
      return processBundleInstructionIdSupplier(() -> value);
    }

    Builder pCollections(Map<String, RunnerApi.PCollection> value);

    Builder coders(Map<String, RunnerApi.Coder> value);

    Builder windowingStrategies(Map<String, RunnerApi.WindowingStrategy> value);

    Builder runnerCapabilities(Set<String> value);

    Builder pCollectionConsumers(Map<String, List<FnDataReceiver<?>>> value);

    Builder incomingDataEndpoints(Map<ApiServiceDescriptor, List<DataEndpoint<?>>> value);

    Builder incomingTimerEndpoints(List<TimerEndpoint<?>> value);

    Builder startBundleFunctions(List<ThrowingRunnable> value);

    Builder finishBundleFunctions(List<ThrowingRunnable> value);

    Builder resetFunctions(List<ThrowingRunnable> value);

    Builder tearDownFunctions(List<ThrowingRunnable> value);

    Builder bundleProgressReporters(List<BundleProgressReporter> value);

    Builder splitListener(BundleSplitListener value);

    Builder bundleFinalizer(BundleFinalizer value);

    Builder outboundAggregators(Map<ApiServiceDescriptor, BeamFnDataOutboundAggregator> value);

    Builder outgoingDataEndpoints(Map<ApiServiceDescriptor, List<DataEndpoint<?>>> value);

    Builder outgoingTimersEndpoints(List<TimerEndpoint<?>> value);

    Builder timerApiServiceDescriptor(ApiServiceDescriptor value);

    PTransformRunnerFactoryTestContext build();
  }

  /** Returns a map from PCollection id to a list of registered consumers. */
  public abstract Map<String, List<FnDataReceiver<?>>> getPCollectionConsumers();

  @Override
  public <T> void addPCollectionConsumer(
      String pCollectionId, FnDataReceiver<WindowedValue<T>> consumer) {
    getPCollectionConsumers()
        .computeIfAbsent(pCollectionId, (unused) -> new ArrayList<>())
        .add(consumer);
  }

  @Override
  public <T> FnDataReceiver<T> getPCollectionConsumer(String pCollectionId) {
    List<FnDataReceiver<?>> receivers = getPCollectionConsumers().get(pCollectionId);
    if (receivers == null) {
      throw new IllegalStateException("No consumers registered for " + pCollectionId);
    } else if (receivers.size() == 1) {
      return (FnDataReceiver<T>) receivers.get(0);
    }
    return new FnDataReceiver<T>() {
      @Override
      public void accept(T input) throws Exception {
        for (FnDataReceiver<?> receiver : receivers) {
          ((FnDataReceiver<T>) receiver).accept(input);
        }
      }
    };
  }

  public abstract Map<ApiServiceDescriptor, List<DataEndpoint<?>>> getIncomingDataEndpoints();

  @Override
  public <T> void addIncomingDataEndpoint(
      ApiServiceDescriptor apiServiceDescriptor, Coder<T> coder, FnDataReceiver<T> receiver) {
    getIncomingDataEndpoints()
        .computeIfAbsent(apiServiceDescriptor, (unused) -> new ArrayList<>())
        .add(DataEndpoint.create(getPTransformId(), coder, receiver));
  }

  public abstract List<TimerEndpoint<?>> getIncomingTimerEndpoints();

  public <T> TimerEndpoint<T> getIncomingTimerEndpoint(String timerFamilyId) {
    for (TimerEndpoint<?> timerEndpoint : getIncomingTimerEndpoints()) {
      if (timerFamilyId.equals(timerEndpoint.getTimerFamilyId())) {
        return (TimerEndpoint<T>) timerEndpoint;
      }
    }
    throw new NoSuchElementException();
  }

  @Override
  public <T> void addIncomingTimerEndpoint(
      String timerFamilyId, Coder<Timer<T>> coder, FnDataReceiver<Timer<T>> receiver) {
    getIncomingTimerEndpoints()
        .add(TimerEndpoint.create(getPTransformId(), timerFamilyId, coder, receiver));
  }

  public abstract Map<ApiServiceDescriptor, BeamFnDataOutboundAggregator> getOutboundAggregators();

  public void addOutboundAggregator(
      ApiServiceDescriptor apiServiceDescriptor, BeamFnDataOutboundAggregator aggregator) {
    getOutboundAggregators().put(apiServiceDescriptor, aggregator);
  }

  public abstract Map<ApiServiceDescriptor, List<DataEndpoint<?>>> getOutgoingDataEndpoints();

  @Override
  public <T> FnDataReceiver<T> addOutgoingDataEndpoint(
      ApiServiceDescriptor apiServiceDescriptor, Coder<T> coder) {
    BeamFnDataOutboundAggregator aggregator = getOutboundAggregators().get(apiServiceDescriptor);
    FnDataReceiver<T> receiver = aggregator.registerOutputDataLocation(getPTransformId(), coder);
    getOutgoingDataEndpoints()
        .computeIfAbsent(apiServiceDescriptor, (unused) -> new ArrayList<>())
        .add(DataEndpoint.create(getPTransformId(), coder, receiver));
    return receiver;
  }

  public abstract List<TimerEndpoint<?>> getOutgoingTimersEndpoints();

  public <T> TimerEndpoint<T> getOutgoingTimersEndpoint(String timerFamilyId) {
    for (TimerEndpoint<?> timerEndpoint : getOutgoingTimersEndpoints()) {
      if (timerFamilyId.equals(timerEndpoint.getTimerFamilyId())) {
        return (TimerEndpoint<T>) timerEndpoint;
      }
    }
    throw new NoSuchElementException();
  }

  @Override
  public <T> FnDataReceiver<Timer<T>> addOutgoingTimersEndpoint(
      String timerFamilyId, Coder<Timer<T>> coder) {
    BeamFnDataOutboundAggregator aggregator =
        getOutboundAggregators().get(getTimerApiServiceDescriptor());
    FnDataReceiver<Timer<T>> receiver =
        aggregator.registerOutputTimersLocation(getPTransformId(), timerFamilyId, coder);
    getOutgoingTimersEndpoints()
        .add(TimerEndpoint.create(getPTransformId(), timerFamilyId, coder, receiver));
    return receiver;
  }

  public abstract ApiServiceDescriptor getTimerApiServiceDescriptor();

  /** Returns a list of methods registered to perform {@link DoFn.StartBundle}. */
  public abstract List<ThrowingRunnable> getStartBundleFunctions();

  @Override
  public void addStartBundleFunction(ThrowingRunnable startBundleFunction) {
    getStartBundleFunctions().add(startBundleFunction);
  }

  /** Returns a list of methods registered to perform {@link DoFn.FinishBundle}. */
  public abstract List<ThrowingRunnable> getFinishBundleFunctions();

  @Override
  public void addFinishBundleFunction(ThrowingRunnable finishBundleFunction) {
    getFinishBundleFunctions().add(finishBundleFunction);
  }

  /**
   * Returns a list of methods registered to be performed after a successful bundle and before the
   * next bundle.
   */
  public abstract List<ThrowingRunnable> getResetFunctions();

  @Override
  public void addResetFunction(ThrowingRunnable resetFunction) {
    getResetFunctions().add(resetFunction);
  }

  /** Returns a list of methods registered to perform {@link DoFn.Teardown}. */
  public abstract List<ThrowingRunnable> getTearDownFunctions();

  @Override
  public void addTearDownFunction(ThrowingRunnable tearDownFunction) {
    getTearDownFunctions().add(tearDownFunction);
  }

  /**
   * Returns a list of methods registered to return additional monitoring data during bundle
   * processing.
   */
  public abstract List<BundleProgressReporter> getBundleProgressReporters();

  @Override
  public void addBundleProgressReporter(BundleProgressReporter bundleProgressReporter) {
    getBundleProgressReporters().add(bundleProgressReporter);
  }
}
