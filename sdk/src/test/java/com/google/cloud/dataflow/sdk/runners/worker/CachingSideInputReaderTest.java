/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.PCollectionViewTesting;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.DirectSideInputReader;
import com.google.cloud.dataflow.sdk.util.PCollectionViewWindow;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.Sized;
import com.google.cloud.dataflow.sdk.util.SizedSideInputReader;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;

/**
 * Tests for {@link CachingSideInputReader}.
 */
@RunWith(JUnit4.class)
public class CachingSideInputReaderTest {

  /**
   * A {@link SizedSideInputReader} where the sizes are included in the values of the
   * {@link PTuple} used to instantiate it.
   */
  private static class SizedDirectSideInputReader extends SizedSideInputReader.Defaults {

    private final SideInputReader subReader;
    private final Map<TupleTag<?>, Long> sizes;

    /**
     * Instantiates a {@link SizedDirectSideInputReader} from a {@link PTuple}. The values in the
     * {@link PTuple} should all be {@link Sized}. A {@link DirectSideInputReader} will be used for
     * the actual retrieval logic; this class merely does the size bookkeeping.
     */
    public SizedDirectSideInputReader(Map<TupleTag<Object>, Sized<Object>> sizedContents) {
      sizes = Maps.newHashMap();
      PTuple values = PTuple.empty();
      for (Map.Entry<TupleTag<Object>, Sized<Object>> entry : sizedContents.entrySet()) {
        values = values.and(entry.getKey(), entry.getValue().getValue());
        sizes.put(entry.getKey(), entry.getValue().getSize());
      }
      subReader = DirectSideInputReader.of(values);
    }

    @Override
    public boolean isEmpty() {
      return subReader.isEmpty();
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return subReader.contains(view);
    }

    @Override
    public <T> Sized<T> getSized(PCollectionView<T> view, BoundedWindow window) {
      return Sized.of(
          subReader.get(view, window),
          sizes.get(view.getTagInternal()));
    }
  }

  private static boolean isCached(
      Cache<PCollectionViewWindow<?>, Sized<Object>> cache,
      PCollectionView<?> view, BoundedWindow window) {
    return null != cache.getIfPresent(PCollectionViewWindow.of(view, window));
  }

  @Test
  public void testCachingSideInputReaderCachesSmallItem() throws Exception {
    Cache<PCollectionViewWindow<?>, Sized<Object>> cache = CacheBuilder.newBuilder()
        .maximumWeight(100)
        .weigher(new SizedWeigher<>(1))
        .build();

    TupleTag<Iterable<WindowedValue<String>>> tag = new TupleTag<>();
    TupleTag<Object> untypedTag = new TupleTag<>(tag.getId());
    PCollectionView<Long> view = PCollectionViewTesting.testingView(
        tag, new PCollectionViewTesting.LengthViewFn<String>(), StringUtf8Coder.of());
    Iterable<WindowedValue<String>> contents =
        PCollectionViewTesting.contentsInDefaultWindow("hello", "goodbye");

    CachingSideInputReader sideInputReader = CachingSideInputReader.of(
        new SizedDirectSideInputReader(ImmutableMap.<TupleTag<Object>, Sized<Object>>of(
            untypedTag, Sized.<Object>of(contents, 5L))),
        cache);

    assertFalse(sideInputReader.isEmpty());
    assertTrue(sideInputReader.contains(view));
    assertFalse(isCached(cache, view, PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW));
    assertThat(
        sideInputReader.get(view, PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW),
        equalTo(2L));
    assertTrue(isCached(cache, view, PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW));
  }

  @Test
  public void testCachingSideInputReaderDoesNotCacheLargeItem() throws Exception {
    Cache<PCollectionViewWindow<?>, Sized<Object>> cache = CacheBuilder.newBuilder()
        .maximumWeight(100)
        .weigher(new SizedWeigher<>(1000))
        .build();

    TupleTag<Iterable<WindowedValue<String>>> tag = new TupleTag<>();
    TupleTag<Object> untypedTag = new TupleTag<>(tag.getId());
    PCollectionView<Long> view = PCollectionViewTesting.testingView(
        tag, new PCollectionViewTesting.LengthViewFn<String>(), StringUtf8Coder.of());
    Iterable<WindowedValue<String>> contents =
        PCollectionViewTesting.contentsInDefaultWindow("hello", "goodbye");

    CachingSideInputReader sideInputReader = CachingSideInputReader.of(
        new SizedDirectSideInputReader(ImmutableMap.<TupleTag<Object>, Sized<Object>>of(
             untypedTag, Sized.<Object>of(contents, 5L))),
        cache);

    assertFalse(sideInputReader.isEmpty());
    assertTrue(sideInputReader.contains(view));
    assertFalse(isCached(cache, view, PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW));
    assertThat(
        sideInputReader.get(view, PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW),
        equalTo(2L));
    assertFalse(isCached(cache, view, PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW));
  }

  @Test
  public void testCachingSideInputReaderEmpty() throws Exception {
    Cache<PCollectionViewWindow<?>, Sized<Object>> cache = CacheBuilder.newBuilder()
        .build();

    TupleTag<Iterable<WindowedValue<String>>> tag = new TupleTag<>();
    PCollectionView<Long> view = PCollectionViewTesting.testingView(
        tag, new PCollectionViewTesting.LengthViewFn<String>(), StringUtf8Coder.of());

    CachingSideInputReader sideInputReader = CachingSideInputReader.of(
        new SizedDirectSideInputReader(ImmutableMap.<TupleTag<Object>, Sized<Object>>of()),
        cache);

    assertFalse(sideInputReader.contains(view));
    assertTrue(sideInputReader.isEmpty());
  }
}
