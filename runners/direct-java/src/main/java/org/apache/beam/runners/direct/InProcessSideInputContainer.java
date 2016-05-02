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
package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.direct.InProcessEvaluationContext.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.PCollectionViewWindow;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.common.base.MoreObjects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

/**
 * An in-process container for {@link PCollectionView PCollectionViews}, which provides methods for
 * constructing {@link SideInputReader SideInputReaders} which block until a side input is
 * available and writing to a {@link PCollectionView}.
 */
class InProcessSideInputContainer {
  private final InProcessEvaluationContext evaluationContext;
  private final Collection<PCollectionView<?>> containedViews;
  private final LoadingCache<PCollectionViewWindow<?>,
      SettableFuture<Iterable<? extends WindowedValue<?>>>> viewByWindows;

  /**
   * Create a new {@link InProcessSideInputContainer} with the provided views and the provided
   * context.
   */
  public static InProcessSideInputContainer create(
      InProcessEvaluationContext context, Collection<PCollectionView<?>> containedViews) {
    CacheLoader<PCollectionViewWindow<?>, SettableFuture<Iterable<? extends WindowedValue<?>>>>
        loader = new CacheLoader<PCollectionViewWindow<?>,
            SettableFuture<Iterable<? extends WindowedValue<?>>>>() {
          @Override
          public SettableFuture<Iterable<? extends WindowedValue<?>>> load(
              PCollectionViewWindow<?> view) {
            return SettableFuture.create();
          }
        };
    LoadingCache<PCollectionViewWindow<?>, SettableFuture<Iterable<? extends WindowedValue<?>>>>
        viewByWindows = CacheBuilder.newBuilder().build(loader);
    return new InProcessSideInputContainer(context, containedViews, viewByWindows);
  }

  private InProcessSideInputContainer(InProcessEvaluationContext context,
      Collection<PCollectionView<?>> containedViews,
      LoadingCache<PCollectionViewWindow<?>, SettableFuture<Iterable<? extends WindowedValue<?>>>>
      viewByWindows) {
    this.evaluationContext = context;
    this.containedViews = ImmutableSet.copyOf(containedViews);
    this.viewByWindows = viewByWindows;
  }

  /**
   * Return a view of this {@link InProcessSideInputContainer} that contains only the views in the
   * provided argument. The returned {@link InProcessSideInputContainer} is unmodifiable without
   * casting, but will change as this {@link InProcessSideInputContainer} is modified.
   */
  public ReadyCheckingSideInputReader createReaderForViews(
      Collection<PCollectionView<?>> newContainedViews) {
    if (!containedViews.containsAll(newContainedViews)) {
      Set<PCollectionView<?>> currentlyContained = ImmutableSet.copyOf(containedViews);
      Set<PCollectionView<?>> newRequested = ImmutableSet.copyOf(newContainedViews);
      throw new IllegalArgumentException("Can't create a SideInputReader with unknown views "
          + Sets.difference(newRequested, currentlyContained));
    }
    return new SideInputContainerSideInputReader(newContainedViews);
  }

  /**
   * Write the provided values to the provided view.
   *
   * <p>The windowed values are first exploded, then for each window the pane is determined. For
   * each window, if the pane is later than the current pane stored within this container, write
   * all of the values to the container as the new values of the {@link PCollectionView}.
   *
   * <p>The provided iterable is expected to contain only a single window and pane.
   */
  public void write(PCollectionView<?> view, Iterable<? extends WindowedValue<?>> values) {
    Map<BoundedWindow, Collection<WindowedValue<?>>> valuesPerWindow =
        indexValuesByWindow(values);
    for (Map.Entry<BoundedWindow, Collection<WindowedValue<?>>> windowValues :
        valuesPerWindow.entrySet()) {
      updatePCollectionViewWindowValues(view, windowValues.getKey(), windowValues.getValue());
    }
  }

  /**
   * Index the provided values by all {@link BoundedWindow windows} in which they appear.
   */
  private Map<BoundedWindow, Collection<WindowedValue<?>>> indexValuesByWindow(
      Iterable<? extends WindowedValue<?>> values) {
    Map<BoundedWindow, Collection<WindowedValue<?>>> valuesPerWindow = new HashMap<>();
    for (WindowedValue<?> value : values) {
      for (BoundedWindow window : value.getWindows()) {
        Collection<WindowedValue<?>> windowValues = valuesPerWindow.get(window);
        if (windowValues == null) {
          windowValues = new ArrayList<>();
          valuesPerWindow.put(window, windowValues);
        }
        windowValues.add(value);
      }
    }
    return valuesPerWindow;
  }

  /**
   * Set the value of the {@link PCollectionView} in the {@link BoundedWindow} to be based on the
   * specified values, if the values are part of a later pane than currently exist within the
   * {@link PCollectionViewWindow}.
   */
  private void updatePCollectionViewWindowValues(
      PCollectionView<?> view, BoundedWindow window, Collection<WindowedValue<?>> windowValues) {
    PCollectionViewWindow<?> windowedView = PCollectionViewWindow.of(view, window);
    SettableFuture<Iterable<? extends WindowedValue<?>>> future = null;
    try {
      future = viewByWindows.get(windowedView);
      if (future.isDone()) {
        Iterator<? extends WindowedValue<?>> existingValues = future.get().iterator();
        PaneInfo newPane = windowValues.iterator().next().getPane();
        // The current value may have no elements, if no elements were produced for the window,
        // but we are recieving late data.
        if (!existingValues.hasNext()
            || newPane.getIndex() > existingValues.next().getPane().getIndex()) {
          viewByWindows.invalidate(windowedView);
          viewByWindows.get(windowedView).set(windowValues);
        }
      } else {
        future.set(windowValues);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      if (future != null && !future.isDone()) {
        future.set(Collections.<WindowedValue<?>>emptyList());
      }
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  private final class SideInputContainerSideInputReader implements ReadyCheckingSideInputReader {
    private final Collection<PCollectionView<?>> readerViews;

    private SideInputContainerSideInputReader(Collection<PCollectionView<?>> readerViews) {
      this.readerViews = ImmutableSet.copyOf(readerViews);
    }

    @Override
    public boolean isReady(final PCollectionView<?> view, final BoundedWindow window) {
      checkArgument(
          readerViews.contains(view),
          "Tried to check if view %s was ready in a SideInputReader that does not contain it. "
              + "Contained views; %s",
          view,
          readerViews);
      return getViewFuture(view, window).isDone();
    }

    @Override
    @Nullable
    public <T> T get(final PCollectionView<T> view, final BoundedWindow window) {
      checkArgument(
          readerViews.contains(view), "calling get(PCollectionView) with unknown view: " + view);
      try {
        final Future<Iterable<? extends WindowedValue<?>>> future = getViewFuture(view, window);
        // Safe covariant cast
        @SuppressWarnings("unchecked")
        Iterable<WindowedValue<?>> values = (Iterable<WindowedValue<?>>) future.get();
        return view.fromIterableInternal(values);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return null;
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Gets the future containing the contents of the provided {@link PCollectionView} in the
     * provided {@link BoundedWindow}, setting up a callback to populate the future with empty
     * contents if necessary.
     */
    private <T> Future<Iterable<? extends WindowedValue<?>>> getViewFuture(
        final PCollectionView<T> view, final BoundedWindow window)  {
      PCollectionViewWindow<T> windowedView = PCollectionViewWindow.of(view, window);
      final SettableFuture<Iterable<? extends WindowedValue<?>>> future =
          viewByWindows.getUnchecked(windowedView);

      WindowingStrategy<?, ?> windowingStrategy = view.getWindowingStrategyInternal();
      evaluationContext.scheduleAfterOutputWouldBeProduced(
          view, window, windowingStrategy, new WriteEmptyViewContents(view, window, future));
      return future;
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return readerViews.contains(view);
    }

    @Override
    public boolean isEmpty() {
      return readerViews.isEmpty();
    }
  }

  private static class WriteEmptyViewContents implements Runnable {
    private final PCollectionView<?> view;
    private final BoundedWindow window;
    private final SettableFuture<Iterable<? extends WindowedValue<?>>> future;

    private WriteEmptyViewContents(PCollectionView<?> view, BoundedWindow window,
        SettableFuture<Iterable<? extends WindowedValue<?>>> future) {
      this.future = future;
      this.view = view;
      this.window = window;
    }

    @Override
    public void run() {
      // The requested window has closed without producing elements, so reflect that in
      // the PCollectionView. If set has already been called, will do nothing.
      future.set(Collections.<WindowedValue<?>>emptyList());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("view", view)
          .add("window", window)
          .toString();
    }
  }
}
