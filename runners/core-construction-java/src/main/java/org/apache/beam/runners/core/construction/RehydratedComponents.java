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
package org.apache.beam.runners.core.construction;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.CoderTranslation.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Vends Java SDK objects rehydrated from a Runner API {@link Components} collection.
 *
 * <p>This ensures maximum memoization of rehydrated components, which is semantically necessary for
 * {@link PCollection} and nice-to-have for other objects.
 */
public class RehydratedComponents {
  private final Components components;

  /**
   * This class may be used in the context of a pipeline or not. If not, then it cannot rehydrate
   * {@link PCollection PCollections}.
   */
  private final @Nullable Pipeline pipeline;

  /**
   * A non-evicting cache, serving as a memo table for rehydrated {@link WindowingStrategy
   * WindowingStrategies}.
   */
  private final LoadingCache<String, WindowingStrategy<?, ?>> windowingStrategies =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<String, WindowingStrategy<?, ?>>() {
                @Override
                public WindowingStrategy<?, ?> load(String id) throws Exception {
                  RunnerApi.@Nullable WindowingStrategy windowingStrategyProto =
                      components.getWindowingStrategiesOrDefault(id, null);
                  checkState(
                      windowingStrategyProto != null,
                      "No WindowingStrategy with id '%s' in serialized components",
                      id);
                  return WindowingStrategyTranslation.fromProto(
                      windowingStrategyProto, RehydratedComponents.this);
                }
              });

  /** A non-evicting cache, serving as a memo table for rehydrated {@link Coder Coders}. */
  private final LoadingCache<String, Coder<?>> coders =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<String, Coder<?>>() {
                @Override
                public Coder<?> load(String id) throws Exception {
                  RunnerApi.@Nullable Coder coder = components.getCodersOrDefault(id, null);
                  checkState(coder != null, "No coder with id '%s' in serialized components", id);
                  return CoderTranslation.fromProto(
                      coder, RehydratedComponents.this, TranslationContext.DEFAULT);
                }
              });

  /**
   * A non-evicting cache, serving as a memo table for rehydrated {@link PCollection PCollections}.
   */
  private final LoadingCache<String, PCollection<?>> pCollections =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<String, PCollection<?>>() {
                @Override
                public PCollection<?> load(String id) throws Exception {
                  checkState(
                      pipeline != null,
                      "%s Cannot rehydrate %s without a %s:"
                          + " provide one via .withPipeline(...)",
                      RehydratedComponents.class.getSimpleName(),
                      PCollection.class.getSimpleName(),
                      Pipeline.class.getSimpleName());
                  return PCollectionTranslation.fromProto(
                          components.getPcollectionsOrThrow(id),
                          pipeline,
                          RehydratedComponents.this)
                      .setName(id);
                }
              });

  /** Create a new {@link RehydratedComponents} from a Runner API {@link Components}. */
  public static RehydratedComponents forComponents(RunnerApi.Components components) {
    return new RehydratedComponents(components, null);
  }

  /** Create a new {@link RehydratedComponents} with a pipeline attached. */
  public RehydratedComponents withPipeline(Pipeline pipeline) {
    return new RehydratedComponents(components, pipeline);
  }

  private RehydratedComponents(RunnerApi.Components components, @Nullable Pipeline pipeline) {
    this.components = components;
    this.pipeline = pipeline;
  }

  /**
   * Returns a {@link PCollection} rehydrated from the Runner API component with the given ID.
   *
   * <p>For a single instance of {@link RehydratedComponents}, this always returns the same instance
   * for a particular id.
   */
  public PCollection<?> getPCollection(String pCollectionId) throws IOException {
    try {
      return pCollections.get(pCollectionId);
    } catch (ExecutionException exc) {
      throw new RuntimeException(exc);
    }
  }

  /**
   * Returns a {@link WindowingStrategy} rehydrated from the Runner API component with the given ID.
   *
   * <p>For a single instance of {@link RehydratedComponents}, this always returns the same instance
   * for a particular id.
   */
  public WindowingStrategy<?, ?> getWindowingStrategy(String windowingStrategyId)
      throws IOException {
    try {
      return windowingStrategies.get(windowingStrategyId);
    } catch (ExecutionException exc) {
      throw new RuntimeException(exc);
    }
  }

  /**
   * Returns a {@link Coder} rehydrated from the Runner API component with the given ID.
   *
   * <p>For a single instance of {@link RehydratedComponents}, this always returns the same instance
   * for a particular id.
   */
  public Coder<?> getCoder(String coderId) throws IOException {
    try {
      return coders.get(coderId);
    } catch (ExecutionException exc) {
      throw new RuntimeException(exc);
    }
  }

  /** Returns the {@link Environment} associated with the given ID. */
  public Environment getEnvironment(String environmentId) {
    return components.getEnvironmentsOrThrow(environmentId);
  }

  public Components getComponents() {
    return components;
  }

  public SdkComponents getSdkComponents(Collection<String> requirements) {
    return SdkComponents.create(
        components,
        Collections.emptyMap(),
        pCollections.asMap(),
        windowingStrategies.asMap(),
        coders.asMap(),
        Collections.emptyMap(),
        requirements);
  }
}
