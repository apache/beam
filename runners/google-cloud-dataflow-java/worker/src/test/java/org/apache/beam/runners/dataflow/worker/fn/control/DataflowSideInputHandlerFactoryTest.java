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
package org.apache.beam.runners.dataflow.worker.fn.control;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.dataflow.worker.DataflowPortabilityPCollectionView;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.MultimapSideInputHandler;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link DataflowSideInputHandlerFactory} */
@RunWith(JUnit4.class)
public final class DataflowSideInputHandlerFactoryTest {

  private static final String TRANSFORM_ID = "transformId";
  private static final String SIDE_INPUT_NAME = "testSideInputId";

  private static final PCollectionView view =
      DataflowPortabilityPCollectionView.with(
          new TupleTag<>(SIDE_INPUT_NAME),
          FullWindowedValueCoder.of(
              KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), GlobalWindow.Coder.INSTANCE));
  private static final RunnerApi.ExecutableStagePayload.SideInputId sideInputId =
      RunnerApi.ExecutableStagePayload.SideInputId.newBuilder()
          .setTransformId(TRANSFORM_ID)
          .setLocalName(SIDE_INPUT_NAME)
          .build();
  private static SideInputReader fakeSideInputReader;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    fakeSideInputReader =
        new SideInputReader() {
          @Nullable
          @Override
          public <T> T get(PCollectionView<T> view, BoundedWindow window) {
            assertEquals(GlobalWindow.INSTANCE, window);
            assertEquals(SIDE_INPUT_NAME, view.getTagInternal().getId());

            return (T)
                InMemoryMultimapSideInputView.fromIterable(
                    StringUtf8Coder.of(),
                    ImmutableList.of(KV.of("foo", 1), KV.of("foo", 4), KV.of("foo", 3)));
          }

          @Override
          public <T> boolean contains(PCollectionView<T> view) {
            return SIDE_INPUT_NAME.equals(view.getTagInternal().getId());
          }

          @Override
          public boolean isEmpty() {
            return false;
          }
        };
  }

  @Test
  public void invalidSideInputThrowsException() {
    ImmutableMap<String, SideInputReader> sideInputReadersMap =
        ImmutableMap.<String, SideInputReader>builder().build();

    ImmutableMap<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
        sideInputIdToPCollectionViewMap =
            ImmutableMap.<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>builder()
                .build();

    DataflowSideInputHandlerFactory factory =
        DataflowSideInputHandlerFactory.of(sideInputReadersMap, sideInputIdToPCollectionViewMap);
    thrown.expect(instanceOf(IllegalStateException.class));
    factory.forMultimapSideInput(
        TRANSFORM_ID,
        SIDE_INPUT_NAME,
        KvCoder.of(VoidCoder.of(), VoidCoder.of()),
        GlobalWindow.Coder.INSTANCE);
  }

  @Test
  public void emptyResultForEmptyCollection() {
    ImmutableMap<String, SideInputReader> sideInputReadersMap =
        ImmutableMap.<String, SideInputReader>builder()
            .put(TRANSFORM_ID, fakeSideInputReader)
            .build();

    ImmutableMap<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
        sideInputIdToPCollectionViewMap =
            ImmutableMap.<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>builder()
                .put(sideInputId, view)
                .build();

    DataflowSideInputHandlerFactory factory =
        DataflowSideInputHandlerFactory.of(sideInputReadersMap, sideInputIdToPCollectionViewMap);
    MultimapSideInputHandler<String, Integer, GlobalWindow> handler =
        factory.forMultimapSideInput(
            TRANSFORM_ID,
            SIDE_INPUT_NAME,
            KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
            GlobalWindow.Coder.INSTANCE);

    Iterable<Integer> result = handler.get("foo2", GlobalWindow.INSTANCE);
    assertThat(result, emptyIterable());
  }

  @Test
  public void multimapSideInputAsIterable() {
    ImmutableMap<String, SideInputReader> sideInputReadersMap =
        ImmutableMap.<String, SideInputReader>builder()
            .put(TRANSFORM_ID, fakeSideInputReader)
            .build();

    ImmutableMap<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
        sideInputIdToPCollectionViewMap =
            ImmutableMap.<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>builder()
                .put(sideInputId, view)
                .build();

    DataflowSideInputHandlerFactory factory =
        DataflowSideInputHandlerFactory.of(sideInputReadersMap, sideInputIdToPCollectionViewMap);

    MultimapSideInputHandler<String, Integer, GlobalWindow> handler =
        factory.forMultimapSideInput(
            TRANSFORM_ID,
            SIDE_INPUT_NAME,
            KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
            GlobalWindow.Coder.INSTANCE);
    Iterable<Integer> result = handler.get("foo", GlobalWindow.INSTANCE);
    assertThat(result, containsInAnyOrder(1, 4, 3));
  }
}
