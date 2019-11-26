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
package org.apache.beam.runners.dataflow;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.BatchViewOverrides.TransformedMap;
import org.apache.beam.runners.dataflow.internal.IsmFormat;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.MetadataKeyCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BatchViewOverrides}. */
@RunWith(JUnit4.class)
public class BatchViewOverridesTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBatchViewAsSingletonToIsmRecord() throws Exception {
    DoFnTester<
            KV<Integer, Iterable<KV<GlobalWindow, WindowedValue<String>>>>,
            IsmRecord<WindowedValue<String>>>
        doFnTester =
            DoFnTester.of(
                new BatchViewOverrides.BatchViewAsSingleton.IsmRecordForSingularValuePerWindowDoFn<
                    String, GlobalWindow>(GlobalWindow.Coder.INSTANCE));

    assertThat(
        doFnTester.processBundle(
            ImmutableList.of(
                KV.of(
                    0, ImmutableList.of(KV.of(GlobalWindow.INSTANCE, valueInGlobalWindow("a")))))),
        contains(IsmRecord.of(ImmutableList.of(GlobalWindow.INSTANCE), valueInGlobalWindow("a"))));
  }

  @Test
  public void testBatchViewAsSingletonToIsmRecordWithMultipleValuesThrowsException()
      throws Exception {
    DoFnTester<
            KV<Integer, Iterable<KV<GlobalWindow, WindowedValue<String>>>>,
            IsmRecord<WindowedValue<String>>>
        doFnTester =
            DoFnTester.of(
                new BatchViewOverrides.BatchViewAsSingleton.IsmRecordForSingularValuePerWindowDoFn<
                    String, GlobalWindow>(GlobalWindow.Coder.INSTANCE));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("found for singleton within window");
    doFnTester.processBundle(
        ImmutableList.of(
            KV.of(
                0,
                ImmutableList.of(
                    KV.of(GlobalWindow.INSTANCE, valueInGlobalWindow("a")),
                    KV.of(GlobalWindow.INSTANCE, valueInGlobalWindow("b"))))));
  }

  @Test
  public void testBatchViewAsListToIsmRecordForGlobalWindow() throws Exception {
    DoFnTester<String, IsmRecord<WindowedValue<String>>> doFnTester =
        DoFnTester.of(
            new BatchViewOverrides.BatchViewAsList.ToIsmRecordForGlobalWindowDoFn<String>());

    // The order of the output elements is important relative to processing order
    assertThat(
        doFnTester.processBundle(ImmutableList.of("a", "b", "c")),
        contains(
            IsmRecord.of(ImmutableList.of(GlobalWindow.INSTANCE, 0L), valueInGlobalWindow("a")),
            IsmRecord.of(ImmutableList.of(GlobalWindow.INSTANCE, 1L), valueInGlobalWindow("b")),
            IsmRecord.of(ImmutableList.of(GlobalWindow.INSTANCE, 2L), valueInGlobalWindow("c"))));
  }

  @Test
  public void testBatchViewAsListToIsmRecordForNonGlobalWindow() throws Exception {
    DoFnTester<
            KV<Integer, Iterable<KV<IntervalWindow, WindowedValue<Long>>>>,
            IsmRecord<WindowedValue<Long>>>
        doFnTester =
            DoFnTester.of(
                new BatchViewOverrides.BatchViewAsList.ToIsmRecordForNonGlobalWindowDoFn<
                    Long, IntervalWindow>(IntervalWindow.getCoder()));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer, Iterable<KV<IntervalWindow, WindowedValue<Long>>>>> inputElements =
        ImmutableList.of(
            KV.of(
                1,
                (Iterable<KV<IntervalWindow, WindowedValue<Long>>>)
                    ImmutableList.of(
                        KV.of(
                            windowA,
                            WindowedValue.of(110L, new Instant(1), windowA, PaneInfo.NO_FIRING)),
                        KV.of(
                            windowA,
                            WindowedValue.of(111L, new Instant(3), windowA, PaneInfo.NO_FIRING)),
                        KV.of(
                            windowA,
                            WindowedValue.of(112L, new Instant(4), windowA, PaneInfo.NO_FIRING)),
                        KV.of(
                            windowB,
                            WindowedValue.of(120L, new Instant(12), windowB, PaneInfo.NO_FIRING)),
                        KV.of(
                            windowB,
                            WindowedValue.of(121L, new Instant(14), windowB, PaneInfo.NO_FIRING)))),
            KV.of(
                2,
                (Iterable<KV<IntervalWindow, WindowedValue<Long>>>)
                    ImmutableList.of(
                        KV.of(
                            windowC,
                            WindowedValue.of(
                                210L, new Instant(25), windowC, PaneInfo.NO_FIRING)))));

    // The order of the output elements is important relative to processing order
    assertThat(
        doFnTester.processBundle(inputElements),
        contains(
            IsmRecord.of(
                ImmutableList.of(windowA, 0L),
                WindowedValue.of(110L, new Instant(1), windowA, PaneInfo.NO_FIRING)),
            IsmRecord.of(
                ImmutableList.of(windowA, 1L),
                WindowedValue.of(111L, new Instant(3), windowA, PaneInfo.NO_FIRING)),
            IsmRecord.of(
                ImmutableList.of(windowA, 2L),
                WindowedValue.of(112L, new Instant(4), windowA, PaneInfo.NO_FIRING)),
            IsmRecord.of(
                ImmutableList.of(windowB, 0L),
                WindowedValue.of(120L, new Instant(12), windowB, PaneInfo.NO_FIRING)),
            IsmRecord.of(
                ImmutableList.of(windowB, 1L),
                WindowedValue.of(121L, new Instant(14), windowB, PaneInfo.NO_FIRING)),
            IsmRecord.of(
                ImmutableList.of(windowC, 0L),
                WindowedValue.of(210L, new Instant(25), windowC, PaneInfo.NO_FIRING))));
  }

  @Test
  public void testToIsmRecordForMapLikeDoFn() throws Exception {
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForSizeTag = new TupleTag<>();
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForEntrySetTag = new TupleTag<>();

    Coder<Long> keyCoder = VarLongCoder.of();
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1,
            2,
            ImmutableList.of(
                MetadataKeyCoder.of(keyCoder), IntervalWindow.getCoder(), BigEndianLongCoder.of()),
            FullWindowedValueCoder.of(VarLongCoder.of(), windowCoder));

    DoFnTester<
            KV<Integer, Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>>,
            IsmRecord<WindowedValue<Long>>>
        doFnTester =
            DoFnTester.of(
                new BatchViewOverrides.BatchViewAsMultimap.ToIsmRecordForMapLikeDoFn<>(
                    outputForSizeTag,
                    outputForEntrySetTag,
                    windowCoder,
                    keyCoder,
                    ismCoder,
                    false /* unique keys */));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer, Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>>>
        inputElements =
            ImmutableList.of(
                KV.of(
                    1,
                    (Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>)
                        ImmutableList.of(
                            KV.of(
                                KV.of(1L, windowA),
                                WindowedValue.of(
                                    110L, new Instant(1), windowA, PaneInfo.NO_FIRING)),
                            // same window same key as to previous
                            KV.of(
                                KV.of(1L, windowA),
                                WindowedValue.of(
                                    111L, new Instant(2), windowA, PaneInfo.NO_FIRING)),
                            // same window different key as to previous
                            KV.of(
                                KV.of(2L, windowA),
                                WindowedValue.of(
                                    120L, new Instant(3), windowA, PaneInfo.NO_FIRING)),
                            // different window same key as to previous
                            KV.of(
                                KV.of(2L, windowB),
                                WindowedValue.of(
                                    210L, new Instant(11), windowB, PaneInfo.NO_FIRING)),
                            // different window and different key as to previous
                            KV.of(
                                KV.of(3L, windowB),
                                WindowedValue.of(
                                    220L, new Instant(12), windowB, PaneInfo.NO_FIRING)))),
                KV.of(
                    2,
                    (Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>)
                        ImmutableList.of(
                            // different shard
                            KV.of(
                                KV.of(4L, windowC),
                                WindowedValue.of(
                                    330L, new Instant(21), windowC, PaneInfo.NO_FIRING)))));

    // The order of the output elements is important relative to processing order
    assertThat(
        doFnTester.processBundle(inputElements),
        contains(
            IsmRecord.of(
                ImmutableList.of(1L, windowA, 0L),
                WindowedValue.of(110L, new Instant(1), windowA, PaneInfo.NO_FIRING)),
            IsmRecord.of(
                ImmutableList.of(1L, windowA, 1L),
                WindowedValue.of(111L, new Instant(2), windowA, PaneInfo.NO_FIRING)),
            IsmRecord.of(
                ImmutableList.of(2L, windowA, 0L),
                WindowedValue.of(120L, new Instant(3), windowA, PaneInfo.NO_FIRING)),
            IsmRecord.of(
                ImmutableList.of(2L, windowB, 0L),
                WindowedValue.of(210L, new Instant(11), windowB, PaneInfo.NO_FIRING)),
            IsmRecord.of(
                ImmutableList.of(3L, windowB, 0L),
                WindowedValue.of(220L, new Instant(12), windowB, PaneInfo.NO_FIRING)),
            IsmRecord.of(
                ImmutableList.of(4L, windowC, 0L),
                WindowedValue.of(330L, new Instant(21), windowC, PaneInfo.NO_FIRING))));

    // Verify the number of unique keys per window.
    assertThat(
        doFnTester.takeOutputElements(outputForSizeTag),
        contains(
            KV.of(
                ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowA)),
                KV.of(windowA, 2L)),
            KV.of(
                ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowB)),
                KV.of(windowB, 2L)),
            KV.of(
                ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowC)),
                KV.of(windowC, 1L))));

    // Verify the output for the unique keys.
    assertThat(
        doFnTester.takeOutputElements(outputForEntrySetTag),
        contains(
            KV.of(
                ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowA)),
                KV.of(windowA, 1L)),
            KV.of(
                ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowA)),
                KV.of(windowA, 2L)),
            KV.of(
                ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowB)),
                KV.of(windowB, 2L)),
            KV.of(
                ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowB)),
                KV.of(windowB, 3L)),
            KV.of(
                ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowC)),
                KV.of(windowC, 4L))));
  }

  @Test
  public void testToIsmRecordForMapLikeDoFnWithoutUniqueKeysThrowsException() throws Exception {
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForSizeTag = new TupleTag<>();
    TupleTag<KV<Integer, KV<IntervalWindow, Long>>> outputForEntrySetTag = new TupleTag<>();

    Coder<Long> keyCoder = VarLongCoder.of();
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1,
            2,
            ImmutableList.of(
                MetadataKeyCoder.of(keyCoder), IntervalWindow.getCoder(), BigEndianLongCoder.of()),
            FullWindowedValueCoder.of(VarLongCoder.of(), windowCoder));

    DoFnTester<
            KV<Integer, Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>>,
            IsmRecord<WindowedValue<Long>>>
        doFnTester =
            DoFnTester.of(
                new BatchViewOverrides.BatchViewAsMultimap.ToIsmRecordForMapLikeDoFn<>(
                    outputForSizeTag,
                    outputForEntrySetTag,
                    windowCoder,
                    keyCoder,
                    ismCoder,
                    true /* unique keys */));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));

    Iterable<KV<Integer, Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>>>
        inputElements =
            ImmutableList.of(
                KV.of(
                    1,
                    (Iterable<KV<KV<Long, IntervalWindow>, WindowedValue<Long>>>)
                        ImmutableList.of(
                            KV.of(
                                KV.of(1L, windowA),
                                WindowedValue.of(
                                    110L, new Instant(1), windowA, PaneInfo.NO_FIRING)),
                            // same window same key as to previous
                            KV.of(
                                KV.of(1L, windowA),
                                WindowedValue.of(
                                    111L, new Instant(2), windowA, PaneInfo.NO_FIRING)))));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unique keys are expected but found key");
    doFnTester.processBundle(inputElements);
  }

  @Test
  public void testToIsmMetadataRecordForSizeDoFn() throws Exception {

    Coder<Long> keyCoder = VarLongCoder.of();
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1,
            2,
            ImmutableList.of(
                MetadataKeyCoder.of(keyCoder), IntervalWindow.getCoder(), BigEndianLongCoder.of()),
            FullWindowedValueCoder.of(VarLongCoder.of(), windowCoder));

    DoFnTester<KV<Integer, Iterable<KV<IntervalWindow, Long>>>, IsmRecord<WindowedValue<Long>>>
        doFnTester =
            DoFnTester.of(
                new BatchViewOverrides.BatchViewAsMultimap.ToIsmMetadataRecordForSizeDoFn<
                    Long, Long, IntervalWindow>(windowCoder));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer, Iterable<KV<IntervalWindow, Long>>>> inputElements =
        ImmutableList.of(
            KV.of(
                1,
                (Iterable<KV<IntervalWindow, Long>>)
                    ImmutableList.of(KV.of(windowA, 2L), KV.of(windowA, 3L), KV.of(windowB, 7L))),
            KV.of(
                ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowB)),
                (Iterable<KV<IntervalWindow, Long>>) ImmutableList.of(KV.of(windowC, 9L))));

    // The order of the output elements is important relative to processing order
    assertThat(
        doFnTester.processBundle(inputElements),
        contains(
            IsmRecord.<WindowedValue<Long>>meta(
                ImmutableList.of(IsmFormat.getMetadataKey(), windowA, 0L),
                CoderUtils.encodeToByteArray(VarLongCoder.of(), 5L)),
            IsmRecord.<WindowedValue<Long>>meta(
                ImmutableList.of(IsmFormat.getMetadataKey(), windowB, 0L),
                CoderUtils.encodeToByteArray(VarLongCoder.of(), 7L)),
            IsmRecord.<WindowedValue<Long>>meta(
                ImmutableList.of(IsmFormat.getMetadataKey(), windowC, 0L),
                CoderUtils.encodeToByteArray(VarLongCoder.of(), 9L))));
  }

  @Test
  public void testToIsmMetadataRecordForKeyDoFn() throws Exception {

    Coder<Long> keyCoder = VarLongCoder.of();
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    IsmRecordCoder<WindowedValue<Long>> ismCoder =
        IsmRecordCoder.of(
            1,
            2,
            ImmutableList.of(
                MetadataKeyCoder.of(keyCoder), IntervalWindow.getCoder(), BigEndianLongCoder.of()),
            FullWindowedValueCoder.of(VarLongCoder.of(), windowCoder));

    DoFnTester<KV<Integer, Iterable<KV<IntervalWindow, Long>>>, IsmRecord<WindowedValue<Long>>>
        doFnTester =
            DoFnTester.of(
                new BatchViewOverrides.BatchViewAsMultimap.ToIsmMetadataRecordForKeyDoFn<
                    Long, Long, IntervalWindow>(keyCoder, windowCoder));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer, Iterable<KV<IntervalWindow, Long>>>> inputElements =
        ImmutableList.of(
            KV.of(
                1,
                (Iterable<KV<IntervalWindow, Long>>)
                    ImmutableList.of(
                        KV.of(windowA, 2L),
                        // same window as previous
                        KV.of(windowA, 3L),
                        // different window as previous
                        KV.of(windowB, 3L))),
            KV.of(
                ismCoder.hash(ImmutableList.of(IsmFormat.getMetadataKey(), windowB)),
                (Iterable<KV<IntervalWindow, Long>>) ImmutableList.of(KV.of(windowC, 3L))));

    // The order of the output elements is important relative to processing order
    assertThat(
        doFnTester.processBundle(inputElements),
        contains(
            IsmRecord.<WindowedValue<Long>>meta(
                ImmutableList.of(IsmFormat.getMetadataKey(), windowA, 1L),
                CoderUtils.encodeToByteArray(VarLongCoder.of(), 2L)),
            IsmRecord.<WindowedValue<Long>>meta(
                ImmutableList.of(IsmFormat.getMetadataKey(), windowA, 2L),
                CoderUtils.encodeToByteArray(VarLongCoder.of(), 3L)),
            IsmRecord.<WindowedValue<Long>>meta(
                ImmutableList.of(IsmFormat.getMetadataKey(), windowB, 1L),
                CoderUtils.encodeToByteArray(VarLongCoder.of(), 3L)),
            IsmRecord.<WindowedValue<Long>>meta(
                ImmutableList.of(IsmFormat.getMetadataKey(), windowC, 1L),
                CoderUtils.encodeToByteArray(VarLongCoder.of(), 3L))));
  }

  @Test
  public void testToMapDoFn() throws Exception {
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    DoFnTester<
            KV<Integer, Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>>,
            IsmRecord<WindowedValue<TransformedMap<Long, WindowedValue<Long>, Long>>>>
        doFnTester =
            DoFnTester.of(
                new BatchViewOverrides.BatchViewAsMap.ToMapDoFn<Long, Long, IntervalWindow>(
                    windowCoder));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer, Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>>>
        inputElements =
            ImmutableList.of(
                KV.of(
                    1,
                    (Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>)
                        ImmutableList.of(
                            KV.of(
                                windowA,
                                WindowedValue.of(
                                    KV.of(1L, 11L), new Instant(3), windowA, PaneInfo.NO_FIRING)),
                            KV.of(
                                windowA,
                                WindowedValue.of(
                                    KV.of(2L, 21L), new Instant(7), windowA, PaneInfo.NO_FIRING)),
                            KV.of(
                                windowB,
                                WindowedValue.of(
                                    KV.of(2L, 21L), new Instant(13), windowB, PaneInfo.NO_FIRING)),
                            KV.of(
                                windowB,
                                WindowedValue.of(
                                    KV.of(3L, 31L),
                                    new Instant(15),
                                    windowB,
                                    PaneInfo.NO_FIRING)))),
                KV.of(
                    2,
                    (Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>)
                        ImmutableList.of(
                            KV.of(
                                windowC,
                                WindowedValue.of(
                                    KV.of(4L, 41L),
                                    new Instant(25),
                                    windowC,
                                    PaneInfo.NO_FIRING)))));

    // The order of the output elements is important relative to processing order
    List<IsmRecord<WindowedValue<TransformedMap<Long, WindowedValue<Long>, Long>>>> output =
        doFnTester.processBundle(inputElements);
    assertEquals(3, output.size());
    Map<Long, Long> outputMap;

    outputMap = output.get(0).getValue().getValue();
    assertEquals(2, outputMap.size());
    assertEquals(ImmutableMap.of(1L, 11L, 2L, 21L), outputMap);

    outputMap = output.get(1).getValue().getValue();
    assertEquals(2, outputMap.size());
    assertEquals(ImmutableMap.of(2L, 21L, 3L, 31L), outputMap);

    outputMap = output.get(2).getValue().getValue();
    assertEquals(1, outputMap.size());
    assertEquals(ImmutableMap.of(4L, 41L), outputMap);
  }

  @Test
  public void testToMultimapDoFn() throws Exception {
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    DoFnTester<
            KV<Integer, Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>>,
            IsmRecord<
                WindowedValue<TransformedMap<Long, Iterable<WindowedValue<Long>>, Iterable<Long>>>>>
        doFnTester =
            DoFnTester.of(
                new BatchViewOverrides.BatchViewAsMultimap.ToMultimapDoFn<
                    Long, Long, IntervalWindow>(windowCoder));

    IntervalWindow windowA = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow windowB = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow windowC = new IntervalWindow(new Instant(20), new Instant(30));

    Iterable<KV<Integer, Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>>>
        inputElements =
            ImmutableList.of(
                KV.of(
                    1,
                    (Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>)
                        ImmutableList.of(
                            KV.of(
                                windowA,
                                WindowedValue.of(
                                    KV.of(1L, 11L), new Instant(3), windowA, PaneInfo.NO_FIRING)),
                            // [BEAM-5184] Specifically test with a duplicate value to ensure that
                            // duplicate key/values are not lost.
                            KV.of(
                                windowA,
                                WindowedValue.of(
                                    KV.of(1L, 11L), new Instant(3), windowA, PaneInfo.NO_FIRING)),
                            KV.of(
                                windowA,
                                WindowedValue.of(
                                    KV.of(1L, 12L), new Instant(5), windowA, PaneInfo.NO_FIRING)),
                            KV.of(
                                windowA,
                                WindowedValue.of(
                                    KV.of(2L, 21L), new Instant(7), windowA, PaneInfo.NO_FIRING)),
                            KV.of(
                                windowB,
                                WindowedValue.of(
                                    KV.of(2L, 21L), new Instant(13), windowB, PaneInfo.NO_FIRING)),
                            KV.of(
                                windowB,
                                WindowedValue.of(
                                    KV.of(3L, 31L),
                                    new Instant(15),
                                    windowB,
                                    PaneInfo.NO_FIRING)))),
                KV.of(
                    2,
                    (Iterable<KV<IntervalWindow, WindowedValue<KV<Long, Long>>>>)
                        ImmutableList.of(
                            KV.of(
                                windowC,
                                WindowedValue.of(
                                    KV.of(4L, 41L),
                                    new Instant(25),
                                    windowC,
                                    PaneInfo.NO_FIRING)))));

    // The order of the output elements is important relative to processing order
    List<
            IsmRecord<
                WindowedValue<TransformedMap<Long, Iterable<WindowedValue<Long>>, Iterable<Long>>>>>
        output = doFnTester.processBundle(inputElements);
    assertEquals(3, output.size());
    Map<Long, Iterable<Long>> outputMap;

    outputMap = output.get(0).getValue().getValue();
    assertEquals(2, outputMap.size());
    assertThat(outputMap.get(1L), containsInAnyOrder(11L, 11L, 12L));
    assertThat(outputMap.get(2L), containsInAnyOrder(21L));

    outputMap = output.get(1).getValue().getValue();
    assertEquals(2, outputMap.size());
    assertThat(outputMap.get(2L), containsInAnyOrder(21L));
    assertThat(outputMap.get(3L), containsInAnyOrder(31L));

    outputMap = output.get(2).getValue().getValue();
    assertEquals(1, outputMap.size());
    assertThat(outputMap.get(4L), containsInAnyOrder(41L));
  }
}
