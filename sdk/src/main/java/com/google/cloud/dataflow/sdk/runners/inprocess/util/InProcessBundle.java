/*
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
 */
package com.google.cloud.dataflow.sdk.runners.inprocess.util;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.Bundle;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.util.ArrayList;
import java.util.Collection;

import javax.annotation.Nullable;

/**
 * A {@link Bundle} that buffers elements in memory.
 */
public final class InProcessBundle<T> implements Bundle<T> {
  private final PCollection<T> pcollection;
  private final boolean keyed;
  private final Object key;
  private Collection<WindowedValue<T>> elements;

  /**
   * Create a new {@link InProcessBundle} for the specified {@link PCollection} without a key.
   */
  public static <T> InProcessBundle<T> unkeyed(PCollection<T> pcollection) {
    return new InProcessBundle<T>(pcollection, false, null);
  }

  /**
   * Create a new {@link InProcessBundle} for the specified {@link PCollection} with the specified
   * key.
   *
   * See {@link #getKey()} and {@link #isKeyed()} for more information.
   */
  public static <T> InProcessBundle<T> keyed(PCollection<T> pcollection, Object key) {
    return new InProcessBundle<T>(pcollection, true, key);
  }

  private InProcessBundle(PCollection<T> pcollection, boolean keyed, Object key) {
    this.pcollection = pcollection;
    this.keyed = keyed;
    this.key = key;
    this.elements = new ArrayList<>();
  }

  @Override
  public InProcessBundle<T> add(WindowedValue<T> element) {
    elements.add(element);
    return this;
  }

  @Override
  @Nullable
  public Object getKey() {
    return key;
  }

  @Override
  public boolean isKeyed() {
    return keyed;
  }

  @Override
  public Iterable<WindowedValue<T>> getElements() {
    return elements;
  }

  @Override
  public PCollection<T> getPCollection() {
    return pcollection;
  }

  @Override
  public String toString() {
    ToStringHelper toStringHelper =
        MoreObjects.toStringHelper(this).add("pcollection", pcollection);
    if (keyed) {
      toStringHelper = toStringHelper.add("key", keyed);
    }
    return toStringHelper.add("elements", elements).toString();
  }
}
