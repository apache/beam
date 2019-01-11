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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Basic side input reader wrapping a {@link PTuple} of side input iterables. Encapsulates
 * conversion according to the {@link PCollectionView} and projection to a particular
 * window.
 */
public class DirectSideInputReader implements SideInputReader {

  private PTuple sideInputValues;

  private DirectSideInputReader(PTuple sideInputValues) {
    this.sideInputValues = sideInputValues;
  }

  public static DirectSideInputReader of(PTuple sideInputValues) {
    return new DirectSideInputReader(sideInputValues);
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputValues.has(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return sideInputValues.isEmpty();
  }

  @Override
  public <T> T get(PCollectionView<T> view, final BoundedWindow window) {
    final TupleTag<Iterable<WindowedValue<?>>> tag = view.getTagInternal();
    if (!sideInputValues.has(tag)) {
      throw new IllegalArgumentException("calling getSideInput() with unknown view");
    }

    if (view.getWindowingStrategyInternal().getWindowFn() instanceof GlobalWindows) {
      return view.getViewFn().apply(sideInputValues.get(tag));
    } else {
      return view.getViewFn().apply(
          Iterables.filter(sideInputValues.get(tag),
              new Predicate<WindowedValue<?>>() {
                  @Override
                  public boolean apply(WindowedValue<?> element) {
                    return element.getWindows().contains(window);
                  }
                }));
    }
  }
}
