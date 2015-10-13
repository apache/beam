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
import com.google.cloud.dataflow.sdk.util.PCollectionViewWindow;
import com.google.cloud.dataflow.sdk.util.Sized;
import com.google.cloud.dataflow.sdk.util.SizedSideInputReader;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CachingSideInputReader}.
 */
@RunWith(JUnit4.class)
public class CachingSideInputReaderTest {

  private static boolean isCached(
      Cache<PCollectionViewWindow<?>, Sized<Object>> cache,
      PCollectionView<?> view, BoundedWindow window) {
    return null != cache.getIfPresent(PCollectionViewWindow.of(view, window));
  }

  /** An arbitrary {@link TupleTag} used for tests. */
  private static final TupleTag<Iterable<WindowedValue<String>>> ITERABLE_TAG = new TupleTag<>();

  /** A {@link TupleTag} that agrees with {@link #ITERABLE_TAG} but is not {@code ==} to it. */
  private static final TupleTag<Object> UNTYPED_ITERABLE_TAG = new TupleTag<>(ITERABLE_TAG.getId());

  /**
   * A {@link PCollectionView} using {@link #ITERABLE_TAG} that maps the contents of a
   * {@link PCollection} to the number of elements it contains.
   */
  private static final PCollectionView<Long> LENGTH_VIEW_FOR_ITERABLE_TAG =
      PCollectionViewTesting.testingView(
          ITERABLE_TAG,
          new PCollectionViewTesting.LengthViewFn<String>(), StringUtf8Coder.of());

  private static final int MAXIMUM_CACHE_SIZE = 1000;

  /** A {@link Cache} that is set up before each test. */
  private Cache<PCollectionViewWindow<?>, Sized<Object>> defaultCache;

  @Before
  public void setupCache() {
    defaultCache = CacheBuilder.newBuilder()
        .maximumWeight(MAXIMUM_CACHE_SIZE)
        .weigher(SizedWeigher.withBaseWeight(1))
        .build();
  }

  @Test
  public void testCachingSideInputReaderAgreesWithUnderlyingReaderForSmallItem() throws Exception {
    // A SideInputReader that vends fixed contents for LENGTH_VIEW_FOR_DEFAULT_TAG
    // with a chosen size that fits in the maximum size of the cache.
    SizedSideInputReader reader = SizedDirectSideInputReader.withContents(
        ImmutableMap.of(
            UNTYPED_ITERABLE_TAG,
            Sized.<Object>of(
                PCollectionViewTesting.contentsInDefaultWindow("some", "small", "collection"),
                MAXIMUM_CACHE_SIZE - 100)));

    // The CachingSideInputReader under test
    CachingSideInputReader cachingReader =
        CachingSideInputReader.of(reader, defaultCache);

    assertThat(
        cachingReader.get(
            LENGTH_VIEW_FOR_ITERABLE_TAG,
            PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW),
        equalTo(
            reader.get(
                LENGTH_VIEW_FOR_ITERABLE_TAG,
                PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW)));
  }

  @Test
  public void testCachingSideInputReaderAgreesWithUnderlyingReaderForLargeItem() throws Exception {
    // A SideInputReader that vends fixed contents for LENGTH_VIEW_FOR_DEFAULT_TAG
    // with a chosen size that exceeds the maximum size of the cache.
    SizedSideInputReader reader = SizedDirectSideInputReader.withContents(
        ImmutableMap.of(
            UNTYPED_ITERABLE_TAG,
            Sized.<Object>of(
                PCollectionViewTesting.contentsInDefaultWindow("some", "large", "collection"),
                MAXIMUM_CACHE_SIZE + 100)));

    // The CachingSideInputReader under test
    CachingSideInputReader cachingReader =
        CachingSideInputReader.of(reader, defaultCache);

    assertThat(
        cachingReader.get(
            LENGTH_VIEW_FOR_ITERABLE_TAG,
            PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW),
        equalTo(
            reader.get(
                LENGTH_VIEW_FOR_ITERABLE_TAG,
                PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW)));
  }


  @Test
  public void testCachingSideInputReaderCachesSmallItem() throws Exception {
    // A SideInputReader that vends fixed contents for LENGTH_VIEW_FOR_DEFAULT_TAG
    // with a chosen size that fits in the maximum size of the cache.
    SizedSideInputReader reader = SizedDirectSideInputReader.withContents(
        ImmutableMap.of(
            UNTYPED_ITERABLE_TAG,
            Sized.<Object>of(
                PCollectionViewTesting.contentsInDefaultWindow("hello", "goodbye"),
                MAXIMUM_CACHE_SIZE - 1000)));

    // The CachingSideInputReader under test
    CachingSideInputReader cachingReader =
        CachingSideInputReader.of(reader, defaultCache);

    cachingReader.get(
        LENGTH_VIEW_FOR_ITERABLE_TAG,
        PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW);

    assertTrue(
        isCached(
            defaultCache,
            LENGTH_VIEW_FOR_ITERABLE_TAG,
            PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW));
  }

  @Test
  public void testCachingSideInputReaderDoesNotCacheLargeItem() throws Exception {
    // A SideInputReader that vends fixed contents for LENGTH_VIEW_FOR_DEFAULT_TAG
    // with a chosen size that exceeds in the maximum size of the cache.
    SizedSideInputReader reader = SizedDirectSideInputReader.withContents(
        ImmutableMap.of(
            UNTYPED_ITERABLE_TAG,
            Sized.<Object>of(
                PCollectionViewTesting.contentsInDefaultWindow("hello", "goodbye"),
                MAXIMUM_CACHE_SIZE + 100)));

    // The CachingSideInputReader under test
    CachingSideInputReader cachingReader =
        CachingSideInputReader.of(reader, defaultCache);

    cachingReader.get(
        LENGTH_VIEW_FOR_ITERABLE_TAG,
        PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW);

    assertFalse(
        isCached(
            defaultCache,
            LENGTH_VIEW_FOR_ITERABLE_TAG,
            PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW));
  }

  @Test
  public void testCachingSideInputReaderEmpty() throws Exception {
    TupleTag<Iterable<WindowedValue<String>>> tag = new TupleTag<>();
    PCollectionView<Long> view = PCollectionViewTesting.testingView(
        tag, new PCollectionViewTesting.LengthViewFn<String>(), StringUtf8Coder.of());

    CachingSideInputReader sideInputReader = CachingSideInputReader.of(
        SizedDirectSideInputReader.withContents(ImmutableMap.<TupleTag<Object>, Sized<Object>>of()),
        defaultCache);

    assertFalse(sideInputReader.contains(view));
    assertTrue(sideInputReader.isEmpty());
  }
}
