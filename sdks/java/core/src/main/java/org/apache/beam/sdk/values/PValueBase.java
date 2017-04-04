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

import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.NameUtils;

/**
 * A {@link PValueBase} is an abstract base class that provides
 * sensible default implementations for methods of {@link PValue}.
 * In particular, this includes functionality for getting/setting:
 *
 * <ul>
 *   <li> The {@link Pipeline} that the {@link PValue} is part of.</li>
 *   <li> Whether the {@link PValue} has bee finalized (as an input
 *     or an output), after which its properties can no longer be changed.</li>
 * </ul>
 *
 * <p>For internal use.
 */
public abstract class PValueBase extends POutputValueBase implements PValue {
  /**
   * Returns the name of this {@link PValueBase}.
   *
   * <p>By default, the name of a {@link PValueBase} is based on the
   * name of the {@link PTransform} that produces it.  It can be
   * specified explicitly by calling {@link #setName}.
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
   * Sets the name of this {@link PValueBase}.  Returns {@code this}.
   *
   * @throws IllegalStateException if this {@link PValueBase} has
   * already been finalized and may no longer be set.
   */
  public PValueBase setName(String name) {
    if (finishedSpecifying) {
      throw new IllegalStateException(
          "cannot change the name of " + this + " once it's been used");
    }
    this.name = name;
    return this;
  }

  /////////////////////////////////////////////////////////////////////////////

  protected PValueBase(Pipeline pipeline) {
    super(pipeline);
  }

  /**
   * No-arg constructor for Java serialization only.
   * The resulting {@link PValueBase} is unlikely to be
   * valid.
   */
  protected PValueBase() {
    super();
  }

  /**
   * The name of this {@link PValueBase}, or null if not yet set.
   */
  private String name;

  /**
   * A local {@link TupleTag} used in the expansion of this {@link PValueBase}.
   */
  private TupleTag<?> tag = new TupleTag<>();

  /**
   * Whether this {@link PValueBase} has been finalized, and its core
   * properties, e.g., name, can no longer be changed.
   */
  private boolean finishedSpecifying = false;

  @Override
  public void recordAsOutput(AppliedPTransform<?, ?, ?> transform) {
    recordAsOutput(transform, "out");
  }

  /**
   * Records that this {@link POutputValueBase} is an output with the
   * given name of the given {@link AppliedPTransform} in the given
   * {@link Pipeline}.
   *
   * <p>To be invoked only by {@link POutput#recordAsOutput}
   * implementations.  Not to be invoked directly by user code.
   */
  protected void recordAsOutput(AppliedPTransform<?, ?, ?> transform,
                                String outName) {
    super.recordAsOutput(transform);
    if (name == null) {
      name = transform.getFullName() + "." + outName;
    }
  }

  /**
   * Returns whether this {@link PValueBase} has been finalized, and
   * its core properties, e.g., name, can no longer be changed.
   *
   * <p>For internal use only.
   */
  public boolean isFinishedSpecifyingInternal() {
    return finishedSpecifying;
  }

  @Override
  public final Map<TupleTag<?>, PValue> expand() {
    return Collections.<TupleTag<?>, PValue>singletonMap(tag, this);
  }

  @Override
  public void finishSpecifying(PInput input, PTransform<?, ?> transform) {
    finishedSpecifying = true;
  }

  @Override
  public String toString() {
    return (name == null ? "<unnamed>" : getName())
        + " [" + getKindString() + "]";
  }

  /**
   * Returns a {@link String} capturing the kind of this
   * {@link PValueBase}.
   *
   * <p>By default, uses the base name of the current class as its kind string.
   */
  protected String getKindString() {
    return NameUtils.approximateSimpleName(getClass());
  }
}
