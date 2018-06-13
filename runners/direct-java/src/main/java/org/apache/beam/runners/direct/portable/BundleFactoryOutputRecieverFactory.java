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

package org.apache.beam.runners.direct.portable;

import java.util.function.Consumer;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * An {@link OutputReceiverFactory} which adds received elements to {@link UncommittedBundle}
 * instances. The produced {@link UncommittedBundle bundles} are added to a provided {@link
 * StepTransformResult.Builder StepTransformResult Builder}.
 */
class BundleFactoryOutputRecieverFactory implements OutputReceiverFactory {
  private final BundleFactory bundleFactory;
  private final RunnerApi.Components components;

  private final Consumer<UncommittedBundle<?>> bundleConsumer;

  private BundleFactoryOutputRecieverFactory(
      BundleFactory bundleFactory,
      Components components,
      Consumer<UncommittedBundle<?>> bundleConsumer) {
    this.bundleFactory = bundleFactory;
    this.components = components;
    this.bundleConsumer = bundleConsumer;
  }

  public static OutputReceiverFactory create(
      BundleFactory bundleFactory,
      Components components,
      Consumer<UncommittedBundle<?>> resultBuilder) {
    return new BundleFactoryOutputRecieverFactory(bundleFactory, components, resultBuilder);
  }

  @Override
  public <OutputT> FnDataReceiver<OutputT> create(String pCollectionId) {
    PCollectionNode pcollection =
        PipelineNode.pCollection(pCollectionId, components.getPcollectionsOrThrow(pCollectionId));
    return create(pcollection);
  }

  private <ElemT, OutputT> FnDataReceiver<OutputT> create(PCollectionNode pcollection) {
    UncommittedBundle<ElemT> bundle = bundleFactory.createBundle(pcollection);
    bundleConsumer.accept(bundle);
    return input -> bundle.add((WindowedValue<ElemT>) input);
  }
}
