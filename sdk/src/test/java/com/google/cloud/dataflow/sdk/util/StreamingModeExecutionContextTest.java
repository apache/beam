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

package com.google.cloud.dataflow.sdk.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.PCollectionViewTesting;
import com.google.cloud.dataflow.sdk.testing.PCollectionViewTesting.ConstantViewFn;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

/**
 * Tests for {@link StreamingModeExecutionContext}.
 */
@RunWith(JUnit4.class)
public class StreamingModeExecutionContextTest {

  @Mock
  StateFetcher stateFetcher;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  // Helper to aid type inference
  private static TupleTag<Iterable<WindowedValue<String>>> newStringTag() {
    return new TupleTag<>();
  }

  /**
   * Tests that the {@link SideInputReader} returned by the {@link StreamingModeExecutionContext}
   * contains the expected views when they are deserialized, as occurs on the
   * service.
   */
  @Test
  public void testSideInputReaderReconstituted() {
    StreamingModeExecutionContext executionContext =
        new StreamingModeExecutionContext("computation", stateFetcher, null);

    PCollectionView<String> preview1 = PCollectionViewTesting.<String, String>testingView(
        newStringTag(), new ConstantViewFn<String, String>("view1"), StringUtf8Coder.of());
    PCollectionView<String> preview2 = PCollectionViewTesting.testingView(
        newStringTag(), new ConstantViewFn<String, String>("view2"), StringUtf8Coder.of());
    PCollectionView<String> preview3 = PCollectionViewTesting.testingView(
        newStringTag(), new ConstantViewFn<String, String>("view3"), StringUtf8Coder.of());

    SideInputReader sideInputReader = executionContext.getSideInputReaderForViews(
        Arrays.asList(preview1, preview2));

    assertTrue(sideInputReader.contains(preview1));
    assertTrue(sideInputReader.contains(preview2));
    assertFalse(sideInputReader.contains(preview3));

    PCollectionView<String> view1 = SerializableUtils.ensureSerializable(preview1);
    PCollectionView<String> view2 = SerializableUtils.ensureSerializable(preview2);
    PCollectionView<String> view3 = SerializableUtils.ensureSerializable(preview3);

    assertTrue(sideInputReader.contains(view1));
    assertTrue(sideInputReader.contains(view2));
    assertFalse(sideInputReader.contains(view3));
  }
}
