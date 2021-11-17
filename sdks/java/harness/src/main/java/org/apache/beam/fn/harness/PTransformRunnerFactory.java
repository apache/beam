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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.BeamFnTimerClient;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;

/** A factory able to instantiate an appropriate handler for a given PTransform. */
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public interface PTransformRunnerFactory<T> {

  /** A context used to instantiate and support the handler necessary to execute the PTransform. */
  interface Context {
    /** Pipeline options. */
    PipelineOptions getPipelineOptions();

    /** A client for handling inbound and outbound data streams. */
    BeamFnDataClient getBeamFnDataClient();

    /** A client for handling state requests. */
    BeamFnStateClient getBeamFnStateClient();

    /** A client for handling inbound and outbound timer streams. */
    BeamFnTimerClient getBeamFnTimerClient();

    /** The id of the PTransform. */
    String getPTransformId();

    /** The PTransform definition. */
    RunnerApi.PTransform getPTransform();

    /** A supplier containing the active process bundle instruction id. */
    Supplier<String> getProcessBundleInstructionIdSupplier();

    /** An immutable mapping from PCollection id to PCollection definition. */
    Map<String, RunnerApi.PCollection> getPCollections();

    /** An immutable mapping from coder id to coder definition. */
    Map<String, RunnerApi.Coder> getCoders();

    /** An immutable mapping from windowing strategy id to windowing strategy definition. */
    Map<String, RunnerApi.WindowingStrategy> getWindowingStrategies();

    /** Register as a consumer for a given PCollection id. */
    <T> void addPCollectionConsumer(
        String pCollectionId, FnDataReceiver<WindowedValue<T>> consumer, Coder<T> valueCoder);

    /** Returns a {@link FnDataReceiver} to send output to for the specified PCollection id. */
    <T> FnDataReceiver<T> getPCollectionConsumer(String pCollectionId);

    /** Register any {@link DoFn.StartBundle} methods. */
    void addStartBundleFunction(ThrowingRunnable startBundleFunction);

    /** Register any {@link DoFn.FinishBundle} methods. */
    void addFinishBundleFunction(ThrowingRunnable finishBundleFunction);

    <T> void addIncomingDataEndpoint(
        Endpoints.ApiServiceDescriptor apiServiceDescriptor,
        Coder<T> coder,
        FnDataReceiver<T> receiver);

    <T> void addIncomingTimerEndpoint(
        String timerFamilyId, Coder<Timer<T>> coder, FnDataReceiver<Timer<T>> receiver);

    /**
     * Register any reset methods. This should not invoke any user code which should be done instead
     * using the {@link #addFinishBundleFunction}. The reset method is guaranteed to be invoked
     * after the bundle completes successfully and after {@code T} becomes ineligible to receive
     * method calls registered with {@link #addProgressRequestCallback} or {@link
     * #getSplitListener}.
     */
    void addResetFunction(ThrowingRunnable resetFunction);

    /**
     * Register a tear down handler. This method will be invoked before {@code T} is eligible to
     * become garbage collected.
     */
    void addTearDownFunction(ThrowingRunnable tearDownFunction);

    /**
     * Register a callback whenever progress is being requested. This method will be called
     * concurrently to any methods registered with {@code pCollectionConsumerRegistry}, {@code
     * startFunctionRegistry}, and {@code finishFunctionRegistry}.
     */
    void addProgressRequestCallback(ProgressRequestCallback progressRequestCallback);

    /**
     * A listener to be invoked when the PTransform splits itself. This method will be called
     * concurrently to any methods registered with {@code pCollectionConsumerRegistry}, {@code
     * startFunctionRegistry}, and {@code finishFunctionRegistry}.
     */
    BundleSplitListener getSplitListener();

    /**
     * Register callbacks that will be invoked when the runner completes the bundle. The specified
     * instant provides the timeout on how long the finalization callback is valid for.
     */
    DoFn.BundleFinalizer getBundleFinalizer();
  }

  /**
   * Creates and returns a handler for a given PTransform. Note that the handler must support
   * processing multiple bundles. The handler will be discarded if bundle processing fails or
   * management of the handler between bundle processing fails. The handler may also be discarded
   * due to memory pressure.
   */
  T createRunnerForPTransform(Context context) throws IOException;

  /**
   * A registrar which can return a mapping from {@link RunnerApi.FunctionSpec#getUrn()} to a
   * factory capable of instantiating an appropriate handler.
   */
  interface Registrar {
    /**
     * Returns a mapping from {@link RunnerApi.FunctionSpec#getUrn()} to a factory capable of
     * instantiating an appropriate handler.
     */
    Map<String, PTransformRunnerFactory> getPTransformRunnerFactories();
  }

  /**
   * A marker interface used to register providing additional monitoring information whenever
   * progress is being requested.
   */
  @FunctionalInterface
  interface ProgressRequestCallback {
    List<MonitoringInfo> getMonitoringInfos() throws Exception;
  }
}
