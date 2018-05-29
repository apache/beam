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

import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A factory able to instantiate an appropriate handler for a given PTransform.
 */
public interface PTransformRunnerFactory<T> {

  /**
   * Creates and returns a handler for a given PTransform. Note that the handler must support
   * processing multiple bundles. The handler will be discarded if an error is thrown during element
   * processing, or during execution of start/finish.
   *
   * @param pipelineOptions Pipeline options
   * @param beamFnDataClient A client for handling inbound and outbound data streams.
   * @param beamFnStateClient A client for handling state requests.
   * @param pTransformId The id of the PTransform.
   * @param pTransform The PTransform definition.
   * @param processBundleInstructionId A supplier containing the active process bundle instruction
   *     id.
   * @param pCollections A mapping from PCollection id to PCollection definition.
   * @param coders A mapping from coder id to coder definition.
   * @param windowingStrategies
   * @param pCollectionIdsToConsumers A mapping from PCollection id to a collection of consumers.
   *     Note that if this handler is a consumer, it should register itself within this multimap
   *     under the appropriate PCollection ids. Also note that all output consumers needed by this
   *     PTransform (based on the values of the {@link PTransform#getOutputsMap()} will have already
   *     registered within this multimap.
   * @param addStartFunction A consumer to register a start bundle handler with.
   * @param addFinishFunction A consumer to register a finish bundle handler with.
   */
  T createRunnerForPTransform(
      PipelineOptions pipelineOptions,
      BeamFnDataClient beamFnDataClient,
      BeamFnStateClient beamFnStateClient,
      String pTransformId,
      RunnerApi.PTransform pTransform,
      Supplier<String> processBundleInstructionId,
      Map<String, PCollection> pCollections,
      Map<String, Coder> coders,
      Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
      Multimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers,
      Consumer<ThrowingRunnable> addStartFunction,
      Consumer<ThrowingRunnable> addFinishFunction)
      throws IOException;

  /**
   * A registrar which can return a mapping from {@link RunnerApi.FunctionSpec#getUrn()} to
   * a factory capable of instantiating an appropriate handler.
   */
  interface Registrar {
    /**
     * Returns a mapping from {@link RunnerApi.FunctionSpec#getUrn()} to a factory capable of
     * instantiating an appropriate handler.
     */
    Map<String, PTransformRunnerFactory> getPTransformRunnerFactories();
  }
}
