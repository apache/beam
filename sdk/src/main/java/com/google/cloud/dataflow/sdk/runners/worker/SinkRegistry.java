/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * An immutable registry from {@link String} identifiers (provided to the worker by the Dataflow
 * service) to appropriate {@link SinkFactory} instances.
 */
public final class SinkRegistry implements SinkFactory {

  public static SinkRegistry defaultRegistry() {
    Map<String, SinkFactory> predefinedSinkFactories = new HashMap<>();
    predefinedSinkFactories.put("TextSink", new TextSinkFactory());
    predefinedSinkFactories.put("com.google.cloud.dataflow.sdk.runners.worker.TextSink",
        new TextSinkFactory());

    predefinedSinkFactories.put("AvroSink", new AvroSinkFactory());
    predefinedSinkFactories.put("com.google.cloud.dataflow.sdk.runners.worker.AvroSink",
        new AvroSinkFactory());

    predefinedSinkFactories.put("IsmSink", new IsmSinkFactory());
    predefinedSinkFactories.put("com.google.cloud.dataflow.sdk.runners.worker.IsmSink",
        new IsmSinkFactory());

    predefinedSinkFactories.put("ShuffleSink", new ShuffleSinkFactory());
    predefinedSinkFactories.put("com.google.cloud.dataflow.sdk.runners.worker.ShuffleSink",
        new ShuffleSinkFactory());

    predefinedSinkFactories.put("PubsubSink", new PubsubSink.Factory());
    predefinedSinkFactories.put("com.google.cloud.dataflow.sdk.runners.worker.PubsubSink",
        new PubsubSink.Factory());

    predefinedSinkFactories.put("WindmillSink", new WindmillSink.Factory());
    predefinedSinkFactories.put("com.google.cloud.dataflow.sdk.runners.worker.WindmillSink",
        new WindmillSink.Factory());

    return new SinkRegistry(predefinedSinkFactories);
  }

  /**
   * Builds a new {@link SinkRegistry} with the provided mutable map of initial mappings.
   *
   * <p>Owns and mutates the provided map, which must be mutable. This constructor should only be
   * called by methods in this class that are aware of this requirement and abstract from this
   * behavior.
   */
  private SinkRegistry(Map<String, SinkFactory> factories) {
    this.factories = factories;
  }

  /**
   * A map from the short names of predefined sinks to the associated {@link SinkFactory}.
   */
  private final Map<String, SinkFactory> factories;

  /**
   * Returns a new {@link SinkRegistry} with the provided identifier associated with the
   * provided {@link SinkFactory}, overriding any existing binding for that identifier.
   */
  public SinkRegistry register(String readerSpecType, SinkFactory factory) {
    Map<String, SinkFactory> newFactories = new HashMap<>();
    newFactories.putAll(factories);
    newFactories.put(readerSpecType, factory);
    return new SinkRegistry(newFactories);
  }

  /**
   * Creates a {@link Sink} from a Dataflow API Sink definition.
   *
   * @throws Exception if the sink could not be decoded and
   * constructed
   */
  @Override
  public Sink<?> create(
      CloudObject sinkSpec,
      Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable ExecutionContext executionContext,
      @Nullable CounterSet.AddCounterMutator addCounterMutator)
      throws Exception {

    String objClassName = sinkSpec.getClassName();

    SinkFactory factory = factories.get(objClassName);
    if (factory == null) {
      throw new IllegalArgumentException(String.format(
          "Unable to create a Sink: Unknown Sink type in specification: %s",
          objClassName));
    }
    return factory.create(sinkSpec, coder, options, executionContext, addCounterMutator);
  }
}
