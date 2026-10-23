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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests dispatch to {@link StatefulParDoTranslatorBatch} and its translation preconditions.
 *
 * <p>These deliberately avoid {@code TestPipeline}: the behaviour under test is translator
 * selection and validation, both of which are decided before any Spark session exists.
 */
@RunWith(JUnit4.class)
public class StatefulParDoTranslatorBatchTest {

  private static final KvCoder<String, Integer> KV_CODER =
      KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of());

  @BeforeClass
  public static void requireSortedGroupsApi() {
    Assume.assumeTrue(
        "Stateful ParDo requires Spark 3.4+", StatefulParDoTranslatorBatch.isSupported());
  }

  // --------------------------------------------------------------------------------------------
  //  Dispatch
  // --------------------------------------------------------------------------------------------

  @Test
  public void appliesToStatefulDoFn() {
    assertTrue(StatefulParDoTranslatorBatch.appliesTo(multiOutput(new StatefulDoFn())));
  }

  @Test
  public void appliesToTimerDoFn() {
    assertTrue(StatefulParDoTranslatorBatch.appliesTo(multiOutput(new TimerDoFn())));
  }

  /**
   * A {@link DoFn} carrying only {@link DoFn.RequiresTimeSortedInput} is not considered stateful by
   * the SDK, so dispatch must test the annotation separately from {@code usesState}/{@code
   * usesTimers}.
   */
  @Test
  public void appliesToTimeSortedOnlyDoFn() {
    assertTrue(StatefulParDoTranslatorBatch.appliesTo(multiOutput(new TimeSortedOnlyDoFn())));
  }

  @Test
  public void doesNotApplyToPlainDoFn() {
    assertFalse(StatefulParDoTranslatorBatch.appliesTo(multiOutput(new PlainDoFn())));
  }

  @Test
  public void registryRoutesStatefulDoFnToStatefulTranslator() {
    TransformTranslator<?, ?, ?> translator =
        new PipelineTranslatorBatch().getTransformTranslator(multiOutput(new StatefulDoFn()));
    assertTrue(
        "Expected StatefulParDoTranslatorBatch but got " + translator,
        translator instanceof StatefulParDoTranslatorBatch);
  }

  @Test
  public void registryRoutesPlainDoFnToParDoTranslator() {
    TransformTranslator<?, ?, ?> translator =
        new PipelineTranslatorBatch().getTransformTranslator(multiOutput(new PlainDoFn()));
    assertTrue(
        "Expected ParDoTranslatorBatch but got " + translator,
        translator instanceof ParDoTranslatorBatch);
  }

  // --------------------------------------------------------------------------------------------
  //  Windowing precondition
  // --------------------------------------------------------------------------------------------

  @Test
  public void rejectsMergingWindows() {
    WindowingStrategy<?, ?> merging =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.standardMinutes(1)));

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                StatefulParDoTranslatorBatch.validateWindowingStrategy(
                    merging, new StatefulDoFn()));
    assertTrue(thrown.getMessage(), thrown.getMessage().contains("merging windows"));
  }

  /**
   * After a {@code GroupByKey} the strategy keeps its merging {@link Sessions} {@code WindowFn} but
   * is flagged as already merged. Such pipelines are legal, so the precondition must test {@code
   * needsMerge()} rather than {@code WindowFn#isNonMerging()}.
   */
  @Test
  public void acceptsWindowsAlreadyMerged() {
    WindowingStrategy<?, ?> alreadyMerged =
        WindowingStrategy.of(Sessions.withGapDuration(Duration.standardMinutes(1)))
            .withAlreadyMerged(true);

    assertFalse("precondition of this test", alreadyMerged.getWindowFn().isNonMerging());
    StatefulParDoTranslatorBatch.validateWindowingStrategy(alreadyMerged, new StatefulDoFn());
  }

  @Test
  public void acceptsNonMergingWindows() {
    StatefulParDoTranslatorBatch.validateWindowingStrategy(
        WindowingStrategy.of(FixedWindows.of(Duration.standardMinutes(1))), new StatefulDoFn());
  }

  // --------------------------------------------------------------------------------------------
  //  Key coder precondition
  // --------------------------------------------------------------------------------------------

  @Test
  public void acceptsDeterministicKeyCoder() {
    StatefulParDoTranslatorBatch.validateKeyCoder(KV_CODER, new StatefulDoFn());
  }

  @Test
  public void rejectsNonKvCoder() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                StatefulParDoTranslatorBatch.validateKeyCoder(
                    StringUtf8Coder.of(), new TimeSortedOnlyDoFn()));
    assertTrue(thrown.getMessage(), thrown.getMessage().contains("KvCoder"));
  }

  /**
   * {@code ParDo} only validates the key coder for {@code DoFns} using state or timers, so a time
   * sorted only {@code DoFn} can reach the translator with a non-deterministic key coder.
   */
  @Test
  public void rejectsNonDeterministicKeyCoder() {
    Coder<KV<String, Integer>> nonDeterministic =
        KvCoder.of(new NonDeterministicStringCoder(), VarIntCoder.of());

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                StatefulParDoTranslatorBatch.validateKeyCoder(
                    nonDeterministic, new TimeSortedOnlyDoFn()));
    assertTrue(thrown.getMessage(), thrown.getMessage().contains("deterministic"));
  }

  // --------------------------------------------------------------------------------------------
  //  Fixtures
  // --------------------------------------------------------------------------------------------

  private static ParDo.MultiOutput<KV<String, Integer>, Integer> multiOutput(
      DoFn<KV<String, Integer>, Integer> doFn) {
    return ParDo.of(doFn).withOutputTags(new TupleTag<Integer>() {}, TupleTagList.empty());
  }

  private static class PlainDoFn extends DoFn<KV<String, Integer>, Integer> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(ctx.element().getValue());
    }
  }

  private static class StatefulDoFn extends DoFn<KV<String, Integer>, Integer> {
    @StateId("value")
    private final StateSpec<ValueState<Integer>> state = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(ProcessContext ctx, @StateId("value") ValueState<Integer> state) {
      ctx.output(ctx.element().getValue());
    }
  }

  private static class TimerDoFn extends DoFn<KV<String, Integer>, Integer> {
    @TimerId("timer")
    private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processElement(ProcessContext ctx, @TimerId("timer") Timer timer) {
      ctx.output(ctx.element().getValue());
    }

    @OnTimer("timer")
    public void onTimer() {}
  }

  private static class TimeSortedOnlyDoFn extends DoFn<KV<String, Integer>, Integer> {
    @RequiresTimeSortedInput
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(ctx.element().getValue());
    }
  }

  /** A String coder that refuses to declare itself deterministic. */
  private static class NonDeterministicStringCoder extends CustomCoder<String> {
    @Override
    public void encode(String value, OutputStream outStream) throws IOException {
      StringUtf8Coder.of().encode(value, outStream);
    }

    @Override
    public String decode(InputStream inStream) throws IOException {
      return StringUtf8Coder.of().decode(inStream);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      throw new NonDeterministicException(this, "not deterministic, by design, for this test");
    }
  }
}
