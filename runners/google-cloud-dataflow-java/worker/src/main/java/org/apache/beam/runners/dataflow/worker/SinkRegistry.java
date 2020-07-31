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

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An immutable registry from {@link String} identifiers (provided to the worker by the Dataflow
 * service) to appropriate {@link SinkFactory} instances.
 */
public final class SinkRegistry implements SinkFactory {

  /**
   * A {@link SinkRegistry} with each {@link SinkFactory} known to the Dataflow worker already
   * registered.
   *
   * <p>Uses {@link ServiceLoader} to dynamically bind well known types to sink factories via {@link
   * SinkFactory.Registrar}.
   */
  public static SinkRegistry defaultRegistry() {
    Set<SinkFactory.Registrar> readerFactoryRegistrars =
        Sets.newTreeSet(ReflectHelpers.ObjectsClassComparator.INSTANCE);
    readerFactoryRegistrars.addAll(
        Lists.newArrayList(
            ServiceLoader.load(SinkFactory.Registrar.class, ReflectHelpers.findClassLoader())));

    ImmutableMap.Builder<String, SinkFactory> factories = ImmutableMap.builder();
    for (SinkFactory.Registrar registrar : readerFactoryRegistrars) {
      factories.putAll(registrar.factories());
    }
    return new SinkRegistry(factories.build());
  }

  /** Builds a new {@link SinkRegistry} with the provided mappings. */
  private SinkRegistry(Map<String, SinkFactory> factories) {
    this.factories = factories;
  }

  /** A map from the short names of predefined sinks to the associated {@link SinkFactory}. */
  private final Map<String, SinkFactory> factories;

  /**
   * Returns a new {@link SinkRegistry} with the provided identifier associated with the provided
   * {@link SinkFactory}, overriding any existing binding for that identifier.
   */
  public SinkRegistry register(String readerSpecType, SinkFactory factory) {
    Map<String, SinkFactory> newFactories = new HashMap<>();
    newFactories.putAll(factories);
    newFactories.put(readerSpecType, factory);
    return new SinkRegistry(newFactories);
  }

  /**
   * Creates a {@link Sink} from a Dataflow API Sink definition. It wraps the sink from the factory
   * in a {@link SizeReportingSinkWrapper}.
   *
   * @throws Exception if the sink could not be decoded and constructed
   */
  @Override
  public SizeReportingSinkWrapper<?> create(
      CloudObject sinkSpec,
      Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    String objClassName = sinkSpec.getClassName();

    SinkFactory factory = factories.get(objClassName);
    if (factory == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to create a Sink: Unknown Sink type in specification: %s", objClassName));
    }

    Sink<?> sink = factory.create(sinkSpec, coder, options, executionContext, operationContext);
    @SuppressWarnings("unchecked")
    Sink<Object> typedSink = (Sink<Object>) sink;
    return new SizeReportingSinkWrapper<>(typedSink, executionContext);
  }
}
