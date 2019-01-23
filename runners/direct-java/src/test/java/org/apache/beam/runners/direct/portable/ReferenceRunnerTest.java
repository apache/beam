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
package org.apache.beam.runners.direct.portable;

import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.beam.runners.core.construction.JavaReadViaImpulse;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.direct.ParDoMultiOverrideFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the {@link ReferenceRunner}. */
@RunWith(JUnit4.class)
public class ReferenceRunnerTest implements Serializable {
  @Test
  public void pipelineExecution() throws Exception {
    Pipeline p = Pipeline.create();
    TupleTag<KV<String, Integer>> food = new TupleTag<>();
    TupleTag<Integer> originals = new TupleTag<Integer>() {};
    PCollectionTuple parDoOutputs =
        p.apply(Create.of(1, 2, 3))
            .apply(
                ParDo.of(
                        new DoFn<Integer, KV<String, Integer>>() {
                          @ProcessElement
                          public void process(@Element Integer e, MultiOutputReceiver r) {
                            for (int i = 0; i < e; i++) {
                              r.get(food)
                                  .outputWithTimestamp(
                                      KV.of("foo", e),
                                      new Instant(0).plus(Duration.standardHours(i)));
                            }
                            r.get(originals).output(e);
                          }
                        })
                    .withOutputTags(food, TupleTagList.of(originals)));
    FixedWindows windowFn = FixedWindows.of(Duration.standardMinutes(5L));
    PCollection<KV<String, Set<Integer>>> grouped =
        parDoOutputs
            .get(food)
            .apply(Window.into(windowFn))
            .apply(GroupByKey.create())
            .apply(
                ParDo.of(
                    new DoFn<KV<String, Iterable<Integer>>, KV<String, Set<Integer>>>() {
                      @ProcessElement
                      public void process(
                          @Element KV<String, Iterable<Integer>> e,
                          OutputReceiver<KV<String, Set<Integer>>> r) {
                        r.output(KV.of(e.getKey(), ImmutableSet.copyOf(e.getValue())));
                      }
                    }));

    PAssert.that(grouped)
        .containsInAnyOrder(
            KV.of("foo", ImmutableSet.of(1, 2, 3)),
            KV.of("foo", ImmutableSet.of(2, 3)),
            KV.of("foo", ImmutableSet.of(3)));

    p.replaceAll(Collections.singletonList(JavaReadViaImpulse.boundedOverride()));

    ReferenceRunner runner =
        ReferenceRunner.forInProcessPipeline(
            PipelineTranslation.toProto(p),
            PipelineOptionsTranslation.toProto(PipelineOptionsFactory.create()));
    runner.execute();
  }

  @Test
  public void testGBK() throws Exception {
    Pipeline p = Pipeline.create();

    PAssert.that(
            p.apply(Create.of(KV.of(42, 0), KV.of(42, 1), KV.of(42, 2)))
                // Will create one bundle for each value, since direct runner uses 1 bundle per key
                .apply(Reshuffle.viaRandomKey())
                // Multiple bundles will emit values onto the same key 42.
                // They must be processed sequentially rather than in parallel, since
                // the trigger firing code expects to receive values sequentially for a key.
                .apply(GroupByKey.create()))
        .satisfies(
            input -> {
              KV<Integer, Iterable<Integer>> kv = Iterables.getOnlyElement(input);
              assertEquals(42, kv.getKey().intValue());
              assertThat(kv.getValue(), containsInAnyOrder(0, 1, 2));
              return null;
            });

    p.replaceAll(Collections.singletonList(JavaReadViaImpulse.boundedOverride()));

    ReferenceRunner runner =
        ReferenceRunner.forInProcessPipeline(
            PipelineTranslation.toProto(p),
            PipelineOptionsTranslation.toProto(PipelineOptionsFactory.create()));
    runner.execute();
  }

  static class PairStringWithIndexToLength extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
      for (long i = tracker.currentRestriction().getFrom(), numIterations = 0;
          tracker.tryClaim(i);
          ++i, ++numIterations) {
        c.output(KV.of(c.element(), (int) i));
        if (numIterations % 3 == 0) {
          return resume();
        }
      }
      return stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange(String element) {
      return new OffsetRange(0, element.length());
    }

    @SplitRestriction
    public void splitRange(
        String element, OffsetRange range, OutputReceiver<OffsetRange> receiver) {
      long middle = (range.getFrom() + range.getTo()) / 2;
      receiver.output(new OffsetRange(range.getFrom(), middle));
      receiver.output(new OffsetRange(middle, range.getTo()));
    }
  }

  @Test
  @Ignore("TODO: BEAM-3743")
  public void testSDF() throws Exception {
    Pipeline p = Pipeline.create();

    PCollection<KV<String, Integer>> res =
        p.apply(Create.of("a", "bb", "ccccc"))
            .apply(ParDo.of(new PairStringWithIndexToLength()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    PAssert.that(res)
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("a", 0),
                KV.of("bb", 0),
                KV.of("bb", 1),
                KV.of("ccccc", 0),
                KV.of("ccccc", 1),
                KV.of("ccccc", 2),
                KV.of("ccccc", 3),
                KV.of("ccccc", 4)));

    p.replaceAll(
        Arrays.asList(
            JavaReadViaImpulse.boundedOverride(),
            PTransformOverride.of(
                PTransformMatchers.splittableParDo(), new ParDoMultiOverrideFactory())));

    ReferenceRunner runner =
        ReferenceRunner.forInProcessPipeline(
            PipelineTranslation.toProto(p),
            PipelineOptionsTranslation.toProto(PipelineOptionsFactory.create()));
    runner.execute();
  }
}
