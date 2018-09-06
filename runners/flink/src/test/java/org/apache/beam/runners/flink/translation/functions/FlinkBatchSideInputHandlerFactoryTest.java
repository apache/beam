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
package org.apache.beam.runners.flink.translation.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.ImmutableExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link FlinkBatchSideInputHandlerFactory}. */
@RunWith(JUnit4.class)
public class FlinkBatchSideInputHandlerFactoryTest {

  private static final String TRANSFORM_ID = "transform-id";
  private static final String SIDE_INPUT_NAME = "side-input";
  private static final String COLLECTION_ID = "collection";
  private static final RunnerApi.FunctionSpec MULTIMAP_ACCESS =
      RunnerApi.FunctionSpec.newBuilder().setUrn(PTransformTranslation.MULTIMAP_SIDE_INPUT).build();
  private static final RunnerApi.FunctionSpec ITERABLE_ACCESS =
      RunnerApi.FunctionSpec.newBuilder().setUrn(PTransformTranslation.ITERABLE_SIDE_INPUT).build();
  private static final ExecutableStage EXECUTABLE_STAGE =
      createExecutableStage(
          Arrays.asList(
              SideInputReference.of(
                  PipelineNode.pTransform(TRANSFORM_ID, RunnerApi.PTransform.getDefaultInstance()),
                  SIDE_INPUT_NAME,
                  PipelineNode.pCollection(
                      COLLECTION_ID, RunnerApi.PCollection.getDefaultInstance()))));
  private static final byte[] ENCODED_NULL = encode(null, VoidCoder.of());
  private static final byte[] ENCODED_FOO = encode("foo", StringUtf8Coder.of());

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private RuntimeContext context;

  @Before
  public void setUpMocks() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void invalidSideInputThrowsException() {
    ExecutableStage stage = createExecutableStage(Collections.emptyList());
    FlinkBatchSideInputHandlerFactory factory =
        FlinkBatchSideInputHandlerFactory.forStage(stage, context);
    thrown.expect(instanceOf(IllegalArgumentException.class));
    factory.forSideInput(
        "transform-id",
        "side-input",
        MULTIMAP_ACCESS,
        KvCoder.of(VoidCoder.of(), VoidCoder.of()),
        GlobalWindow.Coder.INSTANCE);
  }

  @Test
  public void emptyResultForEmptyCollection() {
    FlinkBatchSideInputHandlerFactory factory =
        FlinkBatchSideInputHandlerFactory.forStage(EXECUTABLE_STAGE, context);
    SideInputHandler<Integer, GlobalWindow> handler =
        factory.forSideInput(
            TRANSFORM_ID,
            SIDE_INPUT_NAME,
            MULTIMAP_ACCESS,
            KvCoder.of(VoidCoder.of(), VarIntCoder.of()),
            GlobalWindow.Coder.INSTANCE);
    // We never populated the broadcast variable for "side-input", so the mock will return an empty
    // list.
    Iterable<Integer> result = handler.get(ENCODED_NULL, GlobalWindow.INSTANCE);
    assertThat(result, emptyIterable());
  }

  @Test
  public void singleElementForCollection() {
    when(context.getBroadcastVariable(COLLECTION_ID))
        .thenReturn(
            Arrays.asList(WindowedValue.valueInGlobalWindow(KV.<Void, Integer>of(null, 3))));

    FlinkBatchSideInputHandlerFactory factory =
        FlinkBatchSideInputHandlerFactory.forStage(EXECUTABLE_STAGE, context);
    SideInputHandler<Integer, GlobalWindow> handler =
        factory.forSideInput(
            TRANSFORM_ID,
            SIDE_INPUT_NAME,
            MULTIMAP_ACCESS,
            KvCoder.of(VoidCoder.of(), VarIntCoder.of()),
            GlobalWindow.Coder.INSTANCE);
    Iterable<Integer> result = handler.get(ENCODED_NULL, GlobalWindow.INSTANCE);
    assertThat(result, contains(3));
  }

  @Test
  public void groupsValuesByKey() {
    when(context.getBroadcastVariable(COLLECTION_ID))
        .thenReturn(
            Arrays.asList(
                WindowedValue.valueInGlobalWindow(KV.of("foo", 2)),
                WindowedValue.valueInGlobalWindow(KV.of("bar", 3)),
                WindowedValue.valueInGlobalWindow(KV.of("foo", 5))));

    FlinkBatchSideInputHandlerFactory factory =
        FlinkBatchSideInputHandlerFactory.forStage(EXECUTABLE_STAGE, context);
    SideInputHandler<Integer, GlobalWindow> handler =
        factory.forSideInput(
            TRANSFORM_ID,
            SIDE_INPUT_NAME,
            MULTIMAP_ACCESS,
            KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
            GlobalWindow.Coder.INSTANCE);
    Iterable<Integer> result = handler.get(ENCODED_FOO, GlobalWindow.INSTANCE);
    assertThat(result, containsInAnyOrder(2, 5));
  }

  @Test
  public void groupsValuesByWindowAndKey() {
    Instant instantA = new DateTime(2018, 1, 1, 1, 1, DateTimeZone.UTC).toInstant();
    Instant instantB = new DateTime(2018, 1, 1, 1, 2, DateTimeZone.UTC).toInstant();
    Instant instantC = new DateTime(2018, 1, 1, 1, 3, DateTimeZone.UTC).toInstant();
    IntervalWindow windowA = new IntervalWindow(instantA, instantB);
    IntervalWindow windowB = new IntervalWindow(instantB, instantC);
    when(context.getBroadcastVariable(COLLECTION_ID))
        .thenReturn(
            Arrays.asList(
                WindowedValue.of(KV.of("foo", 1), instantA, windowA, PaneInfo.NO_FIRING),
                WindowedValue.of(KV.of("bar", 2), instantA, windowA, PaneInfo.NO_FIRING),
                WindowedValue.of(KV.of("foo", 3), instantA, windowA, PaneInfo.NO_FIRING),
                WindowedValue.of(KV.of("foo", 4), instantB, windowB, PaneInfo.NO_FIRING),
                WindowedValue.of(KV.of("bar", 5), instantB, windowB, PaneInfo.NO_FIRING),
                WindowedValue.of(KV.of("foo", 6), instantB, windowB, PaneInfo.NO_FIRING)));

    FlinkBatchSideInputHandlerFactory factory =
        FlinkBatchSideInputHandlerFactory.forStage(EXECUTABLE_STAGE, context);
    SideInputHandler<Integer, IntervalWindow> handler =
        factory.forSideInput(
            TRANSFORM_ID,
            SIDE_INPUT_NAME,
            MULTIMAP_ACCESS,
            KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
            IntervalWindowCoder.of());
    Iterable<Integer> resultA = handler.get(ENCODED_FOO, windowA);
    Iterable<Integer> resultB = handler.get(ENCODED_FOO, windowB);
    assertThat(resultA, containsInAnyOrder(1, 3));
    assertThat(resultB, containsInAnyOrder(4, 6));
  }

  @Test
  public void iterableAccessPattern() {
    Instant instantA = new DateTime(2018, 1, 1, 1, 1, DateTimeZone.UTC).toInstant();
    Instant instantB = new DateTime(2018, 1, 1, 1, 2, DateTimeZone.UTC).toInstant();
    Instant instantC = new DateTime(2018, 1, 1, 1, 3, DateTimeZone.UTC).toInstant();
    IntervalWindow windowA = new IntervalWindow(instantA, instantB);
    IntervalWindow windowB = new IntervalWindow(instantB, instantC);
    when(context.getBroadcastVariable(COLLECTION_ID))
        .thenReturn(
            Arrays.asList(
                WindowedValue.of(1, instantA, windowA, PaneInfo.NO_FIRING),
                WindowedValue.of(2, instantA, windowA, PaneInfo.NO_FIRING),
                WindowedValue.of(3, instantB, windowB, PaneInfo.NO_FIRING),
                WindowedValue.of(4, instantB, windowB, PaneInfo.NO_FIRING)));

    FlinkBatchSideInputHandlerFactory factory =
        FlinkBatchSideInputHandlerFactory.forStage(EXECUTABLE_STAGE, context);
    SideInputHandler<Integer, IntervalWindow> handler =
        factory.forSideInput(
            TRANSFORM_ID,
            SIDE_INPUT_NAME,
            ITERABLE_ACCESS,
            VarIntCoder.of(),
            IntervalWindowCoder.of());
    Iterable<Integer> resultA = handler.get(null, windowA);
    Iterable<Integer> resultB = handler.get(null, windowB);
    assertThat(resultA, containsInAnyOrder(1, 2));
    assertThat(resultB, containsInAnyOrder(3, 4));
  }

  private static ExecutableStage createExecutableStage(Collection<SideInputReference> sideInputs) {
    Components components = Components.getDefaultInstance();
    Environment environment = Environment.getDefaultInstance();
    PCollectionNode inputCollection =
        PipelineNode.pCollection("collection-id", RunnerApi.PCollection.getDefaultInstance());
    return ImmutableExecutableStage.of(
        components,
        environment,
        inputCollection,
        sideInputs,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList());
  }

  private static <T> byte[] encode(T value, Coder<T> coder) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      coder.encode(value, out);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }
}
