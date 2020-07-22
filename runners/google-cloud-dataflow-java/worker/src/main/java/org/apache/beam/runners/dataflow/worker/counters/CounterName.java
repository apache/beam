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
package org.apache.beam.runners.dataflow.worker.counters;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.ToStringHelper;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The name of a counter identifies the user-specified name, as well as the origin, the step the
 * counter is associated with, and a prefix to add to the name.
 *
 * <p>For backwards compatibility, the {@link CounterName} will be converted to a flat name (string)
 * during the migration.
 */
@AutoValue
public abstract class CounterName {

  /** Returns a {@link CounterName} with the given name. */
  public static CounterName named(String name) {
    return create(
        name,
        "" /* origin */,
        "" /* stepName */,
        "" /* prefix */,
        null /* contextOriginalName */,
        null /* contextSystemName */,
        null /* originalRequestingStepName */,
        null /* inputIndex */);
  }

  /** Returns a msecs {@link CounterName}. */
  public static CounterName msecs(String name) {
    return named(name + "-msecs");
  }

  /** Returns a {@link CounterName} identical to this, but with the given origin. */
  public CounterName withOrigin(String origin) {
    return create(
        name(),
        origin,
        stepName(),
        prefix(),
        contextOriginalName(),
        contextSystemName(),
        originalRequestingStepName(),
        inputIndex());
  }

  /** Returns a {@link CounterName} identical to this, but with the given step name. */
  public CounterName withStepName(String stepName) {
    return create(
        name(),
        origin(),
        stepName,
        prefix(),
        contextOriginalName(),
        contextSystemName(),
        originalRequestingStepName(),
        inputIndex());
  }

  /** Returns a {@link CounterName} identical to this, but with the given prefix. */
  public CounterName withPrefix(String prefix) {
    return create(
        name(),
        origin(),
        stepName(),
        prefix,
        contextOriginalName(),
        contextSystemName(),
        originalRequestingStepName(),
        inputIndex());
  }

  /**
   * Returns a {@link CounterName} identical to this, but with the {@code originalName} from a
   * {@link NameContext}.
   */
  public CounterName withOriginalName(NameContext context) {
    return create(
        name(),
        origin(),
        stepName(),
        prefix(),
        checkNotNull(context.originalName(), "Expected original name in context %s", context),
        contextSystemName(),
        originalRequestingStepName(),
        inputIndex());
  }

  /**
   * Returns a {@link CounterName} identical to this, but with the {@code originalName} from a
   * {@link String}.
   */
  public CounterName withOriginalName(String originalName) {
    return create(
        name(),
        origin(),
        stepName(),
        prefix(),
        checkNotNull(originalName, "Expected original name in string %s", originalName),
        contextSystemName(),
        originalRequestingStepName(),
        inputIndex());
  }

  /**
   * Returns a {@link CounterName} identical to this, but with the {@code systemName} from a {@link
   * NameContext}.
   */
  public CounterName withSystemName(NameContext context) {
    return create(
        name(),
        origin(),
        stepName(),
        prefix(),
        contextOriginalName(),
        checkNotNull(context.systemName(), "Expected system name in context %s", context),
        originalRequestingStepName(),
        inputIndex());
  }

  /**
   * Returns a {@link CounterName} identical to this, but with the {@code
   * originalRequestingStepName}
   */
  public CounterName withOriginalRequestingStepName(String originalRequestingStepName) {
    return create(
        name(),
        origin(),
        stepName(),
        prefix(),
        contextOriginalName(),
        contextSystemName(),
        checkNotNull(originalRequestingStepName, "Expected a requesting step name"),
        inputIndex());
  }

  public CounterName withInputIndex(Integer inputIndex) {
    return create(
        name(),
        origin(),
        stepName(),
        prefix(),
        contextOriginalName(),
        contextSystemName(),
        originalRequestingStepName(),
        checkNotNull(inputIndex, "Expected an input index"));
  }

  static CounterName create(
      String name,
      String origin,
      String stepName,
      String prefix,
      String contextOriginalName,
      String contextSystemName,
      String originalRequestingStepName,
      Integer inputIndex) {
    return new AutoValue_CounterName(
        checkNotNull(name, "Name is required for CounterName"),
        checkNotNull(origin, "Origin is required for CounterName"),
        checkNotNull(stepName, "StepName is required for CounterName"),
        checkNotNull(prefix, "Prefix is required for CounterName"),
        contextOriginalName,
        contextSystemName,
        originalRequestingStepName,
        inputIndex);
  }

  /**
   * Name of the counter.
   *
   * <p>For example, process-msecs, ElementCount.
   */
  public abstract String name();

  /**
   * Origin (namespace) of counter name.
   *
   * <p>For example, "user" for user-defined counters. It is empty for counters defined by the SDK
   * or the runner.
   */
  public abstract String origin();

  /**
   * System defined step name or the named-output of a step.
   *
   * <p>For example, {@code s1} or {@code s2.out}. It may be empty when counters don't associate
   * with step names.
   */
  public abstract String stepName();

  /**
   * Prefix of group of counters.
   *
   * <p>It is empty when counters don't have general prefixes.
   */
  public abstract String prefix();

  /** An optional {@code contextOriginalName}. */
  public abstract @Nullable String contextOriginalName();

  /** An optional {@code contextSystemName}. */
  public abstract @Nullable String contextSystemName();

  /** An optional {@code originalRequestingStepName}. */
  public abstract @Nullable String originalRequestingStepName();

  /**
   * An optional identifier of a PCollection by index in input list received by step.
   *
   * <p>A CounterName with inputIndex set to 1, and stepName set to 's1' would declare a counter
   * associated to s1's first side input.
   */
  public abstract @Nullable Integer inputIndex();

  /**
   * Flat name is suitable only for hashing.
   *
   * <p>It is null before {@link #getFlatName()} is called. TODO: this can be replaced
   * with @Memoized when v1.4 auto.value is released
   */
  private volatile String flatName;

  public String getFlatName() {
    if (flatName == null) {
      synchronized (this) {
        if (flatName == null) {
          flatName = toString();
        }
      }
    }
    return flatName;
  }

  public String toPrettyString() {
    ToStringHelper helper = MoreObjects.toStringHelper(this);
    helper.omitNullValues();
    helper.add("name", name());
    helper.add("origin", origin());
    helper.add("prefix", prefix());
    helper.add("stepName", stepName());
    helper.add("contextOriginalName", contextOriginalName());
    helper.add("contextSystemName", contextSystemName());
    helper.add("originalRequestingStepName", originalRequestingStepName());
    helper.add("inputIndex", inputIndex());
    return helper.toString();
  }

  /**
   * Pretty name is the equivalent unstructured name.
   *
   * <p>It is null before {@link #getPrettyName()} is called. TODO: this can be replaced
   * with @Memoized when v1.4 auto.value is released
   */
  private volatile String prettyName;

  public String getPrettyName() {
    if (prettyName == null) {
      synchronized (this) {
        if (prettyName == null) {
          prettyName = toPrettyString();
        }
      }
    }
    return prettyName;
  }

  public boolean usesContextOriginalName() {
    return contextOriginalName() != null;
  }

  public boolean usesContextSystemName() {
    return contextSystemName() != null;
  }

  public boolean usesOriginalRequestingStepName() {
    return originalRequestingStepName() != null;
  }

  public boolean isStructured() {
    return usesContextOriginalName() || usesContextSystemName() || usesOriginalRequestingStepName();
  }
}
