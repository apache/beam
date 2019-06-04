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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataflowPortabilityPCollectionView}. */
@RunWith(JUnit4.class)
public class DataflowPortabilityPCollectionViewTest {
  private static final TupleTag<KV<String, String>> TAG = new TupleTag<>("testTag");
  private static final FullWindowedValueCoder<KV<String, String>> CODER =
      FullWindowedValueCoder.of(
          KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), GlobalWindow.Coder.INSTANCE);

  @Test
  public void testValueCoder() {
    assertEquals(
        CODER.getValueCoder(),
        DataflowPortabilityPCollectionView.with(TAG, CODER).getCoderInternal());
  }

  @Test
  public void testWindowCoder() {
    assertEquals(
        CODER.getWindowCoder(),
        DataflowPortabilityPCollectionView.with(TAG, CODER)
            .getWindowingStrategyInternal()
            .getWindowFn()
            .windowCoder());
  }

  @Test
  public void testTag() {
    assertEquals(TAG, DataflowPortabilityPCollectionView.with(TAG, CODER).getTagInternal());
  }

  @Test
  public void testMaterializationUrn() {
    assertEquals(
        Materializations.MULTIMAP_MATERIALIZATION_URN,
        DataflowPortabilityPCollectionView.with(TAG, CODER)
            .getViewFn()
            .getMaterialization()
            .getUrn());
  }
}
