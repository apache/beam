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

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.dataflow.CustomSources;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.CloudSourceUtils;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.Serializer;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.common.collect.Maps;

import java.util.Map;

import javax.annotation.Nullable;

/**
 * Creates a {@link Reader} from a Dataflow API source definition, presented as a
 * {@link CloudObject}.
 */
public interface ReaderFactory {

  /**
   * Creates a {@link Reader} from a Dataflow API source definition, presented as a
   * {@link CloudObject}.
   *
   * @throws Exception if a {@link Reader} could not be created
   */
  Reader<?> create(
      CloudObject cloudSourceSpec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable ExecutionContext executionContext,
      @Nullable CounterSet.AddCounterMutator addCounterMutator,
      @Nullable String operationName)
          throws Exception;

  /**
   * An immutable registry from {@link String} identifiers (provided to the worker by the Dataflow
   * service) to appropriate {@link ReaderFactory} instances.
   */
  public class Registry implements ReaderFactory {

    /**
     * A {@link Registry} with each {@link ReaderFactory} known to the Dataflow worker already
     * registered.
     */
    public static Registry defaultRegistry() {
      Map<String, ReaderFactory> factories = Maps.newHashMap();

      factories.put("TextSource", TextReaderFactory.getInstance());
      factories.put("AvroSource", new AvroReaderFactory());
      factories.put("UngroupedShuffleSource", new UngroupedShuffleReaderFactory());
      factories.put("PartitioningShuffleSource", new PartitioningShuffleReaderFactory());
      factories.put("GroupingShuffleSource", new GroupingShuffleReaderFactory());
      factories.put("InMemorySource", new InMemoryReaderFactory());
      factories.put("BigQuerySource", new BigQueryReaderFactory());

      // Aliases for WindowingWindmillreader
      factories.put("WindowingWindmillReader", new WindowingWindmillReader.Factory());
      factories.put("com.google.cloud.dataflow.sdk.runners.worker.WindowingWindmillReader",
          new WindowingWindmillReader.Factory());
      factories.put("com.google.cloud.dataflow.sdk.runners.worker.BucketingWindmillSource",
          new WindowingWindmillReader.Factory());

      // Aliases for UngroupedWindmillReader
      factories.put("UngroupedWindmillReader", new UngroupedWindmillReader.Factory());
      factories.put("com.google.cloud.dataflow.sdk.runners.worker.UngroupedWindmillSource",
          new UngroupedWindmillReader.Factory());
      factories.put("com.google.cloud.dataflow.sdk.runners.worker.UngroupedWindmillReader",
          new UngroupedWindmillReader.Factory());

      // Aliases for PubsubReader
      factories.put("PubsubReader", new PubsubReader.Factory());
      factories.put("com.google.cloud.dataflow.sdk.runners.worker.PubsubSource",
          new PubsubReader.Factory());

      // Custom sources
      factories.put(
          "com.google.cloud.dataflow.sdk.runners.dataflow.CustomSources",
          new CustomSources.Factory());

      return new Registry(factories);
    }

    /**
     * Builds a new {@link Registry} with the provided mutable map of initial mappings.
     *
     * <p>Owns and mutates the provided map, which must be mutable. This constructor should only be
     * called by methods in this class that are aware of this requirement and abstract from this
     * behavior.
     */
    private Registry(Map<String, ReaderFactory> factories) {
      // ConcatReader requires special treatment: Recursive access to the registry since it calls
      // back to create its sub-readers lazily.
      this.factories = factories;
      this.factories.put(ConcatReader.SOURCE_NAME, ConcatReaderFactory.withRegistry(this));
    }

    /**
     * A map from the short names of predefined sources to the associated {@link ReaderFactory}.
     */
    private final Map<String, ReaderFactory> factories;

    /**
     * Returns a new {@link Registry} with the provided identifier associated with the provided
     * {@link ReaderFactory}, overriding any existing binding for that identifier.
     */
    public Registry register(String readerSpecType, ReaderFactory factory) {
      Map<String, ReaderFactory> newFactories = Maps.newHashMap();
      newFactories.putAll(factories);
      newFactories.put(readerSpecType, factory);
      return new Registry(newFactories);
    }

    /**
     * Creates a {@link Reader} according to the provided {@code sourceSpec}, by dispatching on
     * the type of {@link CloudObject} to instantiate.
     */
    @Override
    public Reader<?> create(
        CloudObject sourceSpec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable ExecutionContext executionContext,
        @Nullable CounterSet.AddCounterMutator addCounterMutator,
        @Nullable String operationName)
            throws Exception {

      String objClassName = sourceSpec.getClassName();
      ReaderFactory readerFactory = factories.get(objClassName);
      if (readerFactory == null) {
        throw new IllegalArgumentException(String.format(
            "Unable to create a Reader: Unknown Reader type in Source specification: %s",
            objClassName));
      }
      return readerFactory.create(
          sourceSpec, coder, options, executionContext, addCounterMutator, operationName);
    }

    /**
     * Creates a {@link Reader} from a Dataflow API {@link Source} specification, using the
     * {@link Coder} contained in the {@link Source} specification.
     */
    public Reader<?> create(
        Source cloudSource,
        @Nullable PipelineOptions options,
        @Nullable ExecutionContext executionContext,
        @Nullable CounterSet.AddCounterMutator addCounterMutator,
        @Nullable String operationName)
            throws Exception {

      cloudSource = CloudSourceUtils.flattenBaseSpecs(cloudSource);
      CloudObject sourceSpec = CloudObject.fromSpec(cloudSource.getSpec());
      Coder<?> coder = null;
      if (cloudSource.getCodec() != null) {
        coder = Serializer.deserialize(cloudSource.getCodec(), Coder.class);
      }
      return create(sourceSpec, coder, options, executionContext, addCounterMutator, operationName);
    }
  }
}
