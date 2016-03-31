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
package org.apache.beam.sdk.util.common;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Strings;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The name of a counter identifies the user-specified name, as well as the origin,
 * the step the counter is associated with, and a prefix to add to the name.
 *
 * <p>For backwards compatibility, the {@link CounterName} will be converted to
 * a flat name (string) during the migration.
 */
public class CounterName {
  /**
   * Returns a {@link CounterName} with the given name.
   */
  public static CounterName named(String name) {
    return new CounterName(name, "", "", "");
  }

  /**
   * Returns a msecs {@link CounterName}.
   */
  public static CounterName msecs(String name) {
    return named(name + "-msecs");
  }

  /**
   * Returns a {@link CounterName} identical to this, but with the given origin.
   */
  public CounterName withOrigin(String origin) {
    return new CounterName(this.name, origin, this.stepName, this.prefix);
  }

  /**
   * Returns a {@link CounterName} identical to this, but with the given step name.
   */
  public CounterName withStepName(String stepName) {
    return new CounterName(this.name, this.origin, stepName, this.prefix);
  }

  /**
   * Returns a {@link CounterName} identical to this, but with the given prefix.
   */
  public CounterName withPrefix(String prefix) {
    return new CounterName(this.name, this.origin, this.stepName, prefix);
  }

  /**
   * Name of the counter.
   *
   * <p>For example, process-msecs, ElementCount.
   */
  private final String name;

  /**
   * Origin (namespace) of counter name.
   *
   * <p>For example, "user" for user-defined counters.
   * It is empty for counters defined by the SDK or the runner.
   */
  private final String origin;

  /**
   * System defined step name or the named-output of a step.
   *
   * <p>For example, {@code s1} or {@code s2.out}.
   * It may be empty when counters don't associate with step names.
   */
  private final String stepName;

  /**
   * Prefix of group of counters.
   *
   * <p>It is empty when counters don't have general prefixes.
   */
  private final String prefix;

  /**
   * Flat name is the equivalent unstructured name.
   *
   * <p>It is null before {@link #getFlatName()} is called.
   */
  private AtomicReference<String> flatName;

  private CounterName(String name, String origin, String stepName, String prefix) {
    this.name = checkNotNull(name, "name");
    this.origin = checkNotNull(origin, "origin");
    this.stepName = checkNotNull(stepName, "stepName");
    this.prefix = checkNotNull(prefix, "prefix");
    this.flatName = new AtomicReference<String>();
  }

  /**
   * Returns the flat name of a structured counter.
   */
  public String getFlatName() {
    String ret = flatName.get();
    if (ret == null) {
      StringBuilder sb = new StringBuilder();
      if (!Strings.isNullOrEmpty(prefix)) {
        // Not all runner versions use "-" to concatenate prefix, it may already have it in it.
        sb.append(prefix);
      }
      if (!Strings.isNullOrEmpty(origin)) {
        sb.append(origin + "-");
      }
      if (!Strings.isNullOrEmpty(stepName)) {
        sb.append(stepName + "-");
      }
      sb.append(name);
      flatName.compareAndSet(null, sb.toString());
      ret = flatName.get();
    }
    return ret;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof CounterName) {
      CounterName that = (CounterName) o;
      return this.getFlatName().equals(that.getFlatName());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getFlatName().hashCode();
  }
}
