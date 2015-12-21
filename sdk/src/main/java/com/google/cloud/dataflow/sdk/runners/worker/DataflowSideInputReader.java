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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.WeightedSideInputReader;
import com.google.cloud.dataflow.sdk.util.WeightedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

/**
 * A simple side input reader that re-reads a side input from its iterable each time it is
 * requested.
 *
 * <p>Sizes are accurate only for {@link PCollectionView} implementations that read the same
 * amount of data for each access.
 */
public class DataflowSideInputReader
    extends WeightedSideInputReader.Defaults
    implements WeightedSideInputReader {

  /** An observer for each side input to count its size as it is being read. */
  private final Map<TupleTag<Object>, ByteSizeObserver> observers;

  /** A byte count saved as overhead per side input, not cleared when the observer is reset. */
  private final Map<TupleTag<Object>, Long> overheads;

  /** A list of TupleTags representing the side input values. */
  private final PTuple sideInputValues;

  private DataflowSideInputReader(
      Iterable<? extends SideInputInfo> sideInputInfos,
      PipelineOptions options,
      ExecutionContext executionContext) throws Exception {
    // Initializing the values may or may not actually read through the
    // source. The full size is the amount read here plus the amount
    // read when view.fromIterableInternal() is called.
    this.observers = new HashMap<>();
    this.overheads = new HashMap<>();

    PTuple sideInputValuesBeingBuilt = PTuple.empty();
    for (SideInputInfo sideInputInfo : sideInputInfos) {
      TupleTag<Object> tag = new TupleTag<>(sideInputInfo.getTag());
      ByteSizeObserver observer = new ByteSizeObserver();
      Object sideInputValue = SideInputUtils.readSideInput(
          options, sideInputInfo, observer, executionContext);
      overheads.put(tag, observer.getBytes());
      observer.reset();
      observers.put(tag, observer);
      sideInputValuesBeingBuilt = sideInputValuesBeingBuilt.and(tag, sideInputValue);
    }
    sideInputValues = sideInputValuesBeingBuilt;
  }

  /**
   * Creates a new {@link SideInputReader} that will provide side inputs
   * according to the provided {@link SideInputInfo} descriptors.
   */
  public static DataflowSideInputReader of(
      Iterable<? extends SideInputInfo> sideInputInfos,
      PipelineOptions options,
      ExecutionContext context)
      throws Exception {
    return new DataflowSideInputReader(sideInputInfos, options, context);
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputValues.has(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return sideInputValues.isEmpty();
  }

  /**
   * Gets a side input for a view and window by reading data according to the corresponding
   * {@link SideInputInfo}, passing the result through the view's
   * {@link PCollectionView#fromIterableInternal} conversion method, and extracting
   * the value for the appropriate window.
   */
  @Override
  public <T> WeightedValue<T> getWeighted(PCollectionView<T> view, final BoundedWindow window) {
    final TupleTag<Iterable<WindowedValue<?>>> tag = view.getTagInternal();
    if (!sideInputValues.has(tag)) {
      throw new IllegalArgumentException("calling getSideInput() with unknown view");
    }

    // It is hard to estimate the size with any accuracy here, and there will be improvements
    // possible, but it is only required to estimate in a way so that a cache will not OOM.
    T value;
    long overhead = overheads.get(tag);
    final ByteSizeObserver observer = observers.get(tag);
    if (view.getWindowingStrategyInternal().getWindowFn() instanceof GlobalWindows) {
      value = view.fromIterableInternal(sideInputValues.get(tag));
      long bytesRead = observer.getBytes();
      observer.reset();
      return WeightedValue.of(value, overhead + bytesRead);
    } else {
      final long[] sum = new long[]{ 0L };
      value = view.fromIterableInternal(
          Iterables.filter(sideInputValues.get(tag),
              new Predicate<WindowedValue<?>>() {
                  @Override
                  public boolean apply(WindowedValue<?> element) {
                    boolean containsWindow = element.getWindows().contains(window);
                    // Only sum up the size of the elements within the window.
                    if (containsWindow) {
                      sum[0] += observer.getBytes();
                    }
                    observer.reset();
                    return containsWindow;
                  }
                }));
      return WeightedValue.of(value, overhead + sum[0]);
    }
  }

  /**
   * An observer for counting the bytes read and then resetting.
   */
  private static class ByteSizeObserver implements Observer {
    /** a byte count beyond overhead, cleared when the observer is reset. */
    private long byteCount = 0;

    @Override
    public void update(Observable reader, Object obj) {
      Preconditions.checkArgument(obj instanceof Long, "unexpected parameter object");
      byteCount = byteCount + (long) obj;
    }

    public void reset() {
      byteCount = 0;
    }

    public long getBytes() {
      return byteCount;
    }
  }
}
