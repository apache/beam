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

import com.google.api.services.dataflow.model.Source;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.util.CloudSourceUtils;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An immutable registry from {@link String} identifiers (provided to the worker by the Dataflow
 * service) to appropriate {@link ReaderFactory} instances.
 */
public class ReaderRegistry implements ReaderFactory {

  /**
   * A {@link ReaderRegistry} with each {@link ReaderFactory} known to the Dataflow worker already
   * registered.
   *
   * <p>Uses {@link ServiceLoader} to dynamically bind well known types to reader factories via a
   * {@link ReaderFactory.Registrar}.
   */
  public static ReaderRegistry defaultRegistry() {
    Set<ReaderFactory.Registrar> readerFactoryRegistrars =
        Sets.newTreeSet(ReflectHelpers.ObjectsClassComparator.INSTANCE);
    readerFactoryRegistrars.addAll(
        Lists.newArrayList(
            ServiceLoader.load(ReaderFactory.Registrar.class, ReflectHelpers.findClassLoader())));

    ImmutableMap.Builder<String, ReaderFactory> factories = ImmutableMap.builder();
    for (ReaderFactory.Registrar registrar : readerFactoryRegistrars) {
      factories.putAll(registrar.factories());
    }
    return new ReaderRegistry(factories.build());
  }

  /** Builds a new {@link ReaderRegistry} with the provided mappings. */
  private ReaderRegistry(Map<String, ReaderFactory> factories) {
    // ConcatReader requires special treatment: Recursive access to the registry since it calls
    // back to create its sub-readers lazily.
    this.factories = new HashMap<>(factories);
    this.factories.put(ConcatReader.SOURCE_NAME, ConcatReaderFactory.withRegistry(this));
  }

  /** A map from the short names of predefined sources to the associated {@link ReaderFactory}. */
  private final Map<String, ReaderFactory> factories;

  /**
   * Returns a new {@link ReaderRegistry} with the provided identifier associated with the provided
   * {@link ReaderFactory}, overriding any existing binding for that identifier.
   */
  public ReaderRegistry register(String readerSpecType, ReaderFactory factory) {
    Map<String, ReaderFactory> newFactories = Maps.newHashMap();
    newFactories.putAll(factories);
    newFactories.put(readerSpecType, factory);
    return new ReaderRegistry(newFactories);
  }

  /**
   * Creates a {@link NativeReader} according to the provided {@code sourceSpec}, by dispatching on
   * the type of {@link CloudObject} to instantiate.
   */
  @Override
  public NativeReader<?> create(
      CloudObject sourceSpec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    ReaderFactory readerFactory = factories.get(sourceSpec.getClassName());
    if (readerFactory == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to create a Reader: Unknown Reader type in Source specification: %s",
              sourceSpec.getClassName()));
    }
    return readerFactory.create(sourceSpec, coder, options, executionContext, operationContext);
  }

  /**
   * Creates a {@link NativeReader} from a Dataflow API {@link Source} specification, using the
   * {@link Coder} contained in the {@link Source} specification.
   */
  public NativeReader<?> create(
      Source cloudSource,
      @Nullable PipelineOptions options,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    cloudSource = CloudSourceUtils.flattenBaseSpecs(cloudSource);
    CloudObject sourceSpec = CloudObject.fromSpec(cloudSource.getSpec());
    Coder<?> coder = null;
    if (cloudSource.getCodec() != null) {
      coder = CloudObjects.coderFromCloudObject(CloudObject.fromSpec(cloudSource.getCodec()));
    }
    return create(sourceSpec, coder, options, executionContext, operationContext);
  }
}
