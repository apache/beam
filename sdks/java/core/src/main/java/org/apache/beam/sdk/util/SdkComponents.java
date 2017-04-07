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

package org.apache.beam.sdk.util;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Components;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/** SDK objects that will be represented at some later point within a {@link Components} object. */
public class SdkComponents {
  private final RunnerApi.Components.Builder componentsBuilder;

  private final BiMap<AppliedPTransform<?, ?, ?>, String> transformIds;
  private final BiMap<PCollection<?>, String> pCollectionIds;
  private final BiMap<WindowingStrategy<?, ?>, String> windowingStrategyIds;
  private final BiMap<Coder<?>, String> coderIds;
  // TODO: Specify environments

  /**
   * Create a new {@link SdkComponents} with no components.
   */
  public static SdkComponents create() {
    return new SdkComponents();
  }

  private SdkComponents() {
    this.componentsBuilder = RunnerApi.Components.newBuilder();
    this.transformIds = HashBiMap.create();
    this.pCollectionIds = HashBiMap.create();
    this.windowingStrategyIds = HashBiMap.create();
    this.coderIds = HashBiMap.create();
  }

  /**
   * Adds the provided {@link AppliedPTransform} into this {@link SdkComponents}.
   *
   * <p>Uses the full name of the transform as the id.
   */
  public String getTransformId(AppliedPTransform<?, ?, ?> pTransform) {
    String existing = transformIds.get(pTransform);
    if (existing != null) {
      return existing;
    }
    String name = pTransform.getFullName();
    if (name.isEmpty()) {
      name = uniqify("unnamed_ptransform", transformIds.values());
    }
    transformIds.put(pTransform, name);
    return name;
  }

  /**
   * Puts the provided {@link PCollection} into this {@link SdkComponents} if it is not already
   * present, returning the id of the provided {@link PCollection} in this {@link SdkComponents}.
   *
   * <p>Uses a unique name based on the result of {@link PCollection#getName()}.
   */
  public String getPCollectionId(PCollection<?> pCollection) {
    String existing = pCollectionIds.get(pCollection);
    if (existing != null) {
      return existing;
    }
    String uniqueName = uniqify(pCollection.getName(), pCollectionIds.values());
    pCollectionIds.put(pCollection, uniqueName);
    return uniqueName;
  }

  /**
   * Puts the provided {@link WindowingStrategy} into this {@link SdkComponents} if it is not
   * already present, returning the id of the {@link WindowingStrategy} in this {@link
   * SdkComponents}.
   */
  public String getWindowingStrategyId(WindowingStrategy<?, ?> windowingStrategy) {
    String existing = windowingStrategyIds.get(windowingStrategy);
    if (existing != null) {
      return existing;
    }
    String baseName = NameUtils.approximateSimpleName(windowingStrategy);
    String name = uniqify(baseName, windowingStrategyIds.values());
    windowingStrategyIds.put(windowingStrategy, name);
    return name;
  }

  /**
   * Puts the provided {@link Coder} into this {@link SdkComponents} if it is not already present,
   * returning the id of the {@link Coder} in this {@link SdkComponents}.
   */
  public String getCoderId(Coder<?> coder) {
    String existing = coderIds.get(coder);
    if (existing != null) {
      return existing;
    }
    String baseName = NameUtils.approximateSimpleName(coder);
    String name = uniqify(baseName, coderIds.values());
    coderIds.put(coder, name);
    return name;
  }

  private String uniqify(String baseName, Set<String> existing) {
    String name = baseName;
    int increment = 1;
    while (existing.contains(name)) {
      name = baseName + Integer.toString(increment);
      increment++;
    }
    return name;
  }

  /**
   * Convert this {@link SdkComponents} into a {@link RunnerApi.Components}, including all of the
   * contained {@link Coder coders}, {@link WindowingStrategy windowing strategies}, {@link
   * PCollection PCollections}, and {@link PTransform PTransforms}.
   */
  @Experimental
  public RunnerApi.Components toComponents() {
    return componentsBuilder.build();
  }
}
