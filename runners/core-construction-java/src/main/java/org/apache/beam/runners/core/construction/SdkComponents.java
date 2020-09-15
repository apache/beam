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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBiMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/** SDK objects that will be represented at some later point within a {@link Components} object. */
public class SdkComponents {
  private final String newIdPrefix;
  private final RunnerApi.Components.Builder componentsBuilder = RunnerApi.Components.newBuilder();

  private final BiMap<AppliedPTransform<?, ?, ?>, String> transformIds = HashBiMap.create();
  private final BiMap<PCollection<?>, String> pCollectionIds = HashBiMap.create();
  private final BiMap<WindowingStrategy<?, ?>, String> windowingStrategyIds = HashBiMap.create();
  private final BiMap<Coder<?>, String> coderIds = HashBiMap.create();
  private final BiMap<Environment, String> environmentIds = HashBiMap.create();
  private final Set<String> requirements;

  private final Set<String> reservedIds = new HashSet<>();

  private String defaultEnvironmentId;

  /** Create a new {@link SdkComponents} with no components. */
  public static SdkComponents create() {
    return new SdkComponents(RunnerApi.Components.getDefaultInstance(), null, "");
  }

  /**
   * Create new {@link SdkComponents} importing all items from provided {@link Components} object.
   *
   * <p>WARNING: This action might cause some of duplicate items created.
   */
  public static SdkComponents create(
      RunnerApi.Components components, Collection<String> requirements) {
    return new SdkComponents(components, requirements, "");
  }

  /*package*/ static SdkComponents create(
      RunnerApi.Components components,
      Map<String, AppliedPTransform<?, ?, ?>> transforms,
      Map<String, PCollection<?>> pCollections,
      Map<String, WindowingStrategy<?, ?>> windowingStrategies,
      Map<String, Coder<?>> coders,
      Map<String, Environment> environments,
      Collection<String> requirements) {
    SdkComponents sdkComponents = SdkComponents.create(components, requirements);
    sdkComponents.transformIds.inverse().putAll(transforms);
    sdkComponents.pCollectionIds.inverse().putAll(pCollections);
    sdkComponents.windowingStrategyIds.inverse().putAll(windowingStrategies);
    sdkComponents.coderIds.inverse().putAll(coders);
    sdkComponents.environmentIds.inverse().putAll(environments);
    return sdkComponents;
  }

  public static SdkComponents create(PipelineOptions options) {
    SdkComponents sdkComponents =
        new SdkComponents(RunnerApi.Components.getDefaultInstance(), null, "");
    PortablePipelineOptions portablePipelineOptions = options.as(PortablePipelineOptions.class);
    sdkComponents.registerEnvironment(
        Environments.createOrGetDefaultEnvironment(portablePipelineOptions));
    return sdkComponents;
  }

  private SdkComponents(
      @Nullable Components components,
      @Nullable Collection<String> requirements,
      String newIdPrefix) {
    this.newIdPrefix = newIdPrefix;
    this.requirements = new HashSet<>();

    if (components == null) {
      if (requirements != null) {
        this.requirements.addAll(requirements);
      }
    } else {
      mergeFrom(components, requirements);
    }
  }

  /** Merge Components proto into this SdkComponents instance. */
  public void mergeFrom(
      RunnerApi.Components components, @Nullable Collection<String> requirements) {
    reservedIds.addAll(components.getTransformsMap().keySet());
    reservedIds.addAll(components.getPcollectionsMap().keySet());
    reservedIds.addAll(components.getWindowingStrategiesMap().keySet());
    reservedIds.addAll(components.getCodersMap().keySet());
    reservedIds.addAll(components.getEnvironmentsMap().keySet());

    components.getEnvironmentsMap().forEach(environmentIds.inverse()::forcePut);

    if (requirements != null) {
      this.requirements.addAll(requirements);
    }

    componentsBuilder.mergeFrom(components);
  }

  /**
   * Returns an SdkComponents like this one, but which will prefix all newly generated ids with the
   * given string.
   *
   * <p>Useful for ensuring independently-constructed components have non-overlapping ids.
   */
  public SdkComponents withNewIdPrefix(String newIdPrefix) {
    SdkComponents sdkComponents =
        new SdkComponents(componentsBuilder.build(), requirements, newIdPrefix);
    sdkComponents.transformIds.putAll(transformIds);
    sdkComponents.pCollectionIds.putAll(pCollectionIds);
    sdkComponents.windowingStrategyIds.putAll(windowingStrategyIds);
    sdkComponents.coderIds.putAll(coderIds);
    sdkComponents.environmentIds.putAll(environmentIds);
    return sdkComponents;
  }

  /**
   * Registers the provided {@link AppliedPTransform} into this {@link SdkComponents}, returning a
   * unique ID for the {@link AppliedPTransform}. Multiple registrations of the same {@link
   * AppliedPTransform} will return the same unique ID.
   *
   * <p>All of the children must already be registered within this {@link SdkComponents}.
   */
  public String registerPTransform(
      AppliedPTransform<?, ?, ?> appliedPTransform, List<AppliedPTransform<?, ?, ?>> children)
      throws IOException {
    String name = getApplicationName(appliedPTransform);
    // If this transform is present in the components, nothing to do. return the existing name.
    // Otherwise the transform must be translated and added to the components.
    if (componentsBuilder.getTransformsOrDefault(name, null) != null) {
      return name;
    }
    checkNotNull(children, "child nodes may not be null");
    componentsBuilder.putTransforms(
        name, PTransformTranslation.toProto(appliedPTransform, children, this));
    return name;
  }

  /**
   * Gets the ID for the provided {@link AppliedPTransform}. The provided {@link AppliedPTransform}
   * will not be added to the components produced by this {@link SdkComponents} until it is
   * translated via {@link #registerPTransform(AppliedPTransform, List)}.
   */
  private String getApplicationName(AppliedPTransform<?, ?, ?> appliedPTransform) {
    String existing = transformIds.get(appliedPTransform);
    if (existing != null) {
      return existing;
    }

    String name = appliedPTransform.getFullName();
    if (name.isEmpty()) {
      name = "unnamed-ptransform";
    }
    name = uniqify(name, transformIds.values());
    transformIds.put(appliedPTransform, name);
    return name;
  }

  String getExistingPTransformId(AppliedPTransform<?, ?, ?> appliedPTransform) {
    checkArgument(
        transformIds.containsKey(appliedPTransform),
        "%s %s has not been previously registered",
        AppliedPTransform.class.getSimpleName(),
        appliedPTransform);
    return transformIds.get(appliedPTransform);
  }

  public String getPTransformIdOrThrow(AppliedPTransform<?, ?, ?> appliedPTransform) {
    String existing = transformIds.get(appliedPTransform);
    checkArgument(existing != null, "PTransform id not found for: %s", appliedPTransform);
    return existing;
  }

  /**
   * Registers the provided {@link PCollection} into this {@link SdkComponents}, returning a unique
   * ID for the {@link PCollection}. Multiple registrations of the same {@link PCollection} will
   * return the same unique ID.
   */
  public String registerPCollection(PCollection<?> pCollection) throws IOException {
    String existing = pCollectionIds.get(pCollection);
    if (existing != null) {
      return existing;
    }
    String uniqueName = uniqify(pCollection.getName(), pCollectionIds.values());
    pCollectionIds.put(pCollection, uniqueName);
    componentsBuilder.putPcollections(
        uniqueName, PCollectionTranslation.toProto(pCollection, this));
    return uniqueName;
  }

  /**
   * Registers the provided {@link WindowingStrategy} into this {@link SdkComponents}, returning a
   * unique ID for the {@link WindowingStrategy}. Multiple registrations of the same {@link
   * WindowingStrategy} will return the same unique ID.
   */
  public String registerWindowingStrategy(WindowingStrategy<?, ?> windowingStrategy)
      throws IOException {
    String existing = windowingStrategyIds.get(windowingStrategy);
    if (existing != null) {
      return existing;
    }
    String baseName =
        String.format(
            "%s(%s)",
            NameUtils.approximateSimpleName(windowingStrategy),
            NameUtils.approximateSimpleName(windowingStrategy.getWindowFn()));
    String name = uniqify(baseName, windowingStrategyIds.values());
    windowingStrategyIds.put(windowingStrategy, name);
    RunnerApi.WindowingStrategy windowingStrategyProto =
        WindowingStrategyTranslation.toProto(windowingStrategy, this);
    componentsBuilder.putWindowingStrategies(name, windowingStrategyProto);
    return name;
  }

  /**
   * Registers the provided {@link Coder} into this {@link SdkComponents}, returning a unique ID for
   * the {@link Coder}. Multiple registrations of the same {@link Coder} will return the same unique
   * ID.
   *
   * <p>Coders are stored by identity to ensure that coders with implementations of {@link
   * #equals(Object)} and {@link #hashCode()} but incompatible binary formats are not considered the
   * same coder.
   */
  public String registerCoder(Coder<?> coder) throws IOException {
    String existing = coderIds.get(coder);
    if (existing != null) {
      return existing;
    }
    String baseName = NameUtils.approximateSimpleName(coder);
    String name = uniqify(baseName, coderIds.values());
    coderIds.put(coder, name);
    RunnerApi.Coder coderProto = CoderTranslation.toProto(coder, this);
    componentsBuilder.putCoders(name, coderProto);
    return name;
  }

  /**
   * Registers the provided {@link Environment} into this {@link SdkComponents}, returning a unique
   * ID for the {@link Environment}. Multiple registrations of the same {@link Environment} will
   * return the same unique ID.
   */
  public String registerEnvironment(Environment env) {
    String environmentId;
    String existing = environmentIds.get(env);
    if (existing != null) {
      environmentId = existing;
    } else {
      String name = uniqify(env.getUrn(), environmentIds.values());
      environmentIds.put(env, name);
      componentsBuilder.putEnvironments(name, env);
      environmentId = name;
    }
    if (defaultEnvironmentId == null) {
      defaultEnvironmentId = environmentId;
    }
    return environmentId;
  }

  public String getOnlyEnvironmentId() {
    // TODO Support multiple environments. The environment should be decided by the translation.
    if (defaultEnvironmentId != null) {
      return defaultEnvironmentId;
    } else {
      return Iterables.getOnlyElement(componentsBuilder.getEnvironmentsMap().keySet());
    }
  }

  public void addRequirement(String urn) {
    requirements.add(urn);
  }

  private String uniqify(String baseName, Set<String> existing) {
    String name = newIdPrefix + baseName;
    int increment = 1;
    while (existing.contains(name) || reservedIds.contains(name)) {
      name = newIdPrefix + baseName + Integer.toString(increment);
      increment++;
    }
    return name;
  }

  /**
   * Convert this {@link SdkComponents} into a {@link RunnerApi.Components}, including all of the
   * contained {@link Coder coders}, {@link WindowingStrategy windowing strategies}, {@link
   * PCollection PCollections}, and {@link PTransform PTransforms}.
   */
  public RunnerApi.Components toComponents() {
    return componentsBuilder.build();
  }

  public Collection<String> requirements() {
    return ImmutableSet.copyOf(requirements);
  }
}
