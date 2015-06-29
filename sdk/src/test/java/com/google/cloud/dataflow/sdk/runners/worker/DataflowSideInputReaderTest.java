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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.PCollectionViewTesting;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.Sized;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link DataflowSideInputReader}.
 */
@RunWith(JUnit4.class)
public class DataflowSideInputReaderTest {

  private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();

  /**
   * The size, in bytes, of a {@code long} placed in
   * {@link PCollectionViewTesting#DEFAULT_NONEMPTY_WINDOW}. This is the size of each of the
   * elements of each {@link PCollection} created in the following tests.
   *
   * <p>This value is arbitrary from the point of view of these tests.
   * The correctness of {@link DataflowSideInputReader} does not depend on this value,
   * but depends on the fact that the reported sizes are this value times the number
   * of elements in a collection.
   */
  private long windowedLongBytes() throws Exception {
    long arbitraryLong = 42L;
    return CoderUtils.encodeToByteArray(
        PCollectionViewTesting.defaultWindowedValueCoder(LONG_CODER),
        PCollectionViewTesting.valueInDefaultWindow(arbitraryLong)).length;
  }

  /**
   * Creates a {@link Source} descriptor for reading the provided contents as a side input.
   * The contents will all be placed in the {@link PCollectionViewTesting#DEFAULT_NONEMPTY_WINDOW}.
   *
   * <p>If the {@link PCollectionView} has an incompatible {@link Coder} or
   * {@link WindowingStrategy}, then results are unpredictable.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private <T> Source sourceInDefaultWindow(PCollectionView<T> view, T... values)
      throws Exception {
    List<WindowedValue<T>> windowedValues =
        Lists.newArrayList(PCollectionViewTesting.contentsInDefaultWindow(values));

    List<Coder<?>> componentCoders = (List) view.getCoderInternal().getCoderArguments();
    if (componentCoders == null || componentCoders.size() != 1) {
      throw new Exception("Could not extract element Coder from " + view.getCoderInternal());
    }
    Coder<WindowedValue<T>> elemCoder = (Coder<WindowedValue<T>>) componentCoders.get(0);

    return InMemoryReaderFactoryTest.createInMemoryCloudSource(
        windowedValues, null, null, elemCoder);
  }

  /**
   * Tests that when a PCollectionView is actually available in a {@link DataflowSideInputReader},
   * the read succeeds and has the right size.
   */
  @Test
  public void testDataflowSideInputReaderGoodRead() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    ExecutionContext executionContext = DataflowExecutionContext.withoutSideInputs();
    TupleTag<Iterable<WindowedValue<Long>>> tag = new TupleTag<>();
    PCollectionView<Long> view = PCollectionViewTesting.<Long, Long>testingView(
        tag, new PCollectionViewTesting.LengthViewFn<Long>(), LONG_CODER);

    SideInputInfo sideInputInfo = SideInputUtils.createCollectionSideInputInfo(
        sourceInDefaultWindow(view, 1L, -43255L, 0L, 13L, 1975858L));
    sideInputInfo.setTag(view.getTagInternal().getId());

    DataflowSideInputReader sideInputReader = DataflowSideInputReader.of(
        Arrays.asList(sideInputInfo), options, executionContext);

    assertFalse(sideInputReader.isEmpty());
    assertTrue(sideInputReader.contains(view));

    Sized<Long> sizedValue = sideInputReader.getSized(
        view, PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW);
    assertThat(sizedValue.getValue(), equalTo(5L));
    assertThat(sizedValue.getSize(), equalTo(5 * windowedLongBytes()));

    // A repeated read should yield the same size
    Sized<Long> repeatedRead = sideInputReader.getSized(
        view, PCollectionViewTesting.DEFAULT_NONEMPTY_WINDOW);
    assertThat(repeatedRead.getValue(), equalTo(5L));
    assertThat(sizedValue.getSize(), equalTo(5 * windowedLongBytes()));

    // Reading an empty window still yields the same size, for now
    Sized<Long> emptyWindowValue = sideInputReader.getSized(
        view, PCollectionViewTesting.DEFAULT_EMPTY_WINDOW);
    assertThat(emptyWindowValue.getValue(), equalTo(0L));
    assertThat(emptyWindowValue.getSize(), equalTo(5 * windowedLongBytes()));
  }

  /**
   * Tests that when a {@link PCollectionView} is not available in a
   * {@link DataflowSideInputReader}, it is reflected properly.
   */
  @Test
  public void testDataflowSideInputReaderBadRead() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    ExecutionContext executionContext = DataflowExecutionContext.withoutSideInputs();
    TupleTag<Iterable<WindowedValue<Long>>> tag = new TupleTag<>();
    PCollectionView<Long> view = PCollectionViewTesting.testingView(
        tag, new PCollectionViewTesting.LengthViewFn<Long>(), LONG_CODER);

    SideInputInfo sideInputInfo = SideInputUtils.createCollectionSideInputInfo(
        sourceInDefaultWindow(view, 1L, -43255L, 0L, 13L, 1975858L));
    sideInputInfo.setTag("not the same tag at all");

    DataflowSideInputReader sideInputReader = DataflowSideInputReader.of(
        Arrays.asList(sideInputInfo), options, executionContext);

    assertFalse(sideInputReader.contains(view));
  }

  /**
   * Tests that when a {@link PCollectionView} is not available in a
   * {@link DataflowSideInputReader}, it is reflected properly.
   */
  @Test
  public void testDataflowSideInputEmpty() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    ExecutionContext executionContext = DataflowExecutionContext.withoutSideInputs();
    DataflowSideInputReader sideInputReader = DataflowSideInputReader.of(
        Collections.<SideInputInfo>emptyList(), options, executionContext);
    assertTrue(sideInputReader.isEmpty());
  }
}
