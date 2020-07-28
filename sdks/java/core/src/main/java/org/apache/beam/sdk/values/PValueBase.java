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
package org.apache.beam.sdk.values;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.NameUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * <b><i>For internal use. No backwards compatibility guarantees.</i></b>
 *
 * <p>An abstract base class that provides default implementations for some methods of {@link
 * PValue}.
 */
@Internal
public abstract class PValueBase implements PValue {

  private final transient @Nullable Pipeline pipeline;

  /**
   * Returns the name of this {@link PValueBase}.
   *
   * <p>By default, the name of a {@link PValueBase} is based on the name of the {@link PTransform}
   * that produces it. It can be specified explicitly by calling {@link #setName}.
   *
   * @throws IllegalStateException if the name hasn't been set yet
   */
  @Override
  public String getName() {
    if (name == null) {
      throw new IllegalStateException("name not set");
    }
    return name;
  }

  /**
   * Sets the name of this {@link PValueBase}. Returns {@code this}.
   *
   * @throws IllegalStateException if this {@link PValueBase} has already been finalized and may no
   *     longer be set.
   */
  public PValueBase setName(String name) {
    checkState(!finishedSpecifying, "cannot change the name of %s once it's been used", this);
    this.name = name;
    return this;
  }

  /////////////////////////////////////////////////////////////////////////////

  protected PValueBase(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  /**
   * No-arg constructor to allow subclasses to implement {@link java.io.Serializable}. The resulting
   * {@link PValueBase} is not valid as a {@link PValue}, but may have other properties that are
   * still usable, such as the tag in a {@link PCollectionView}.
   */
  protected PValueBase() {
    this.pipeline = null;
  }

  /** The name of this {@link PValueBase}, or {@code null} if not yet set. */
  private @Nullable String name;

  /**
   * Whether this {@link PValueBase} has been finalized, and its core properties, e.g., name, can no
   * longer be changed.
   */
  private boolean finishedSpecifying = false;

  /**
   * Returns whether this {@link PValueBase} has been finalized, and its core properties, e.g.,
   * name, can no longer be changed.
   *
   * <p>For internal use only.
   */
  boolean isFinishedSpecifying() {
    return finishedSpecifying;
  }

  @Override
  public void finishSpecifying(PInput input, PTransform<?, ?> transform) {
    finishedSpecifying = true;
  }

  @Override
  public String toString() {
    return (name == null ? "<unnamed>" : getName()) + " [" + getKindString() + "]";
  }

  /**
   * Returns a {@link String} capturing the kind of this {@link PValueBase}.
   *
   * <p>By default, uses the base name of the current class as its kind string.
   */
  protected String getKindString() {
    return NameUtils.approximateSimpleName(getClass());
  }

  @Override
  public Pipeline getPipeline() {
    checkState(
        pipeline != null,
        "Pipeline was null for %s. "
            + "this probably means it was used as a %s after being deserialized, "
            + "which not unsupported.",
        getClass().getCanonicalName(),
        PValue.class.getSimpleName());
    return pipeline;
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {
    if (name == null) {
      setName(defaultName(transformName));
    }
  }

  static String defaultName(String transformName) {
    return String.format("%s.%s", transformName, "out");
  }
}
