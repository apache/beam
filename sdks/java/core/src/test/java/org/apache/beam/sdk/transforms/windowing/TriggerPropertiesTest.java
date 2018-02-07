package org.apache.beam.sdk.transforms.windowing;

import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TriggerPropertiesTest implements Serializable {
  @Rule public transient TestPipeline p = TestPipeline.create();

  private enum AccumulationMode {
    ACCUMULATING,
    DISCARDING
  }

  @Parameters(name = "{index}: {0}, {1}")
  public static Iterable<Object[]> data() {
    // Test atomic triggers: pane, watermark, time
    List<Trigger> atomicTriggers =
        ImmutableList.<Trigger>of(
            AfterPane.elementCountAtLeast(1),
            AfterPane.elementCountAtLeast(3),
            AfterWatermark.pastEndOfWindow(),
            AfterProcessingTime.pastFirstElementInPane(),
            AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(200)));
    // Test also repeated versions of these
    List<Trigger> atomicAndRepeated = Lists.newArrayList(atomicTriggers);
    for (Trigger trigger : atomicTriggers) {
      atomicAndRepeated.add(Repeatedly.forever(trigger));
    }
    // TODO: Test other composite triggers.

    List<Object[]> res = Lists.newArrayList();
    for (Trigger trigger : atomicAndRepeated) {
      res.add(new Object[] {trigger, AccumulationMode.DISCARDING});
      res.add(new Object[] {trigger, AccumulationMode.ACCUMULATING});
    }
    return res;
  }

  @Parameter(0)
  public Trigger trigger;

  @Parameter(1)
  public AccumulationMode accumulationMode;

  @Test
  @Category({ValidatesRunner.class, UsesTestStream.class})
  public void testContinuationTrigger() {
    int n = 10;
    List<TimestampedValue<KV<String, Integer>>> allElements = Lists.newArrayList();
    for (int i = 0; i < n; ++i) {
      allElements.add(
          TimestampedValue.of(KV.of((i % 2 == 0) ? "a" : "b", i), new Instant(100 * i)));
    }
    Collections.shuffle(allElements);

    TestStream.Builder<KV<String, Integer>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
    for (int i = 0; i < n; ++i) {
      stream =
          stream
              // Processing-time watermark.
              // TODO: test also perfect watermark and others
              .advanceWatermarkTo(new Instant(100 * i))
              .advanceProcessingTime(Duration.millis(100))
              .addElements(allElements.get(i));
    }

    Window<KV<String, Integer>> window =
        Window.<KV<String, Integer>>configure().triggering(trigger);
    PCollection<KV<String, Integer>> input =
        p.apply(stream.advanceWatermarkToInfinity())
            .apply(
                (accumulationMode == AccumulationMode.ACCUMULATING)
                    ? window.accumulatingFiredPanes()
                    : window.discardingFiredPanes());

    PCollection<KV<String, Integer>> regrouped = input.apply(new Regroup<String, Integer>());

    PCollection<KV<String, KV<String, Integer>>> regroupedFirstGBK =
        regrouped.apply("Add firstGBK", WithKeys.<String, KV<String, Integer>>of("firstGBK"));
    PCollection<KV<String, KV<String, Integer>>> regroupedSecondGBK =
        regrouped
            .apply(new RegroupOntoSingleKey<KV<String, Integer>>())
            .apply("Add secondGBK", WithKeys.<String, KV<String, Integer>>of("secondGBK"));

    PAssert.that(
            PCollectionList.of(regroupedFirstGBK)
                .and(regroupedSecondGBK)
                .apply(Flatten.<KV<String, KV<String, Integer>>>pCollections()))
        .satisfies(
            new SerializableFunction<Iterable<KV<String, KV<String, Integer>>>, Void>() {
              @Override
              public Void apply(Iterable<KV<String, KV<String, Integer>>> input) {
                Multiset<KV<String, Integer>> firstGBK = HashMultiset.create();
                Multiset<KV<String, Integer>> secondGBK = HashMultiset.create();
                for (KV<String, KV<String, Integer>> kv : input) {
                  if (kv.getKey().equals("firstGBK")) {
                    firstGBK.add(kv.getValue());
                  } else {
                    secondGBK.add(kv.getValue());
                  }
                }
                System.out.println("firstGBK: " + firstGBK);
                System.out.println("secondGBK: " + secondGBK);
                assertEquals(firstGBK, secondGBK);
                return null;
              }
            });

    p.run();
  }

  private static class Regroup<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {
    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
      return input
          .apply(GroupByKey.<K, V>create())
          .apply(
              ParDo.of(
                  new DoFn<KV<K, Iterable<V>>, KV<K, V>>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                      for (V v : c.element().getValue()) {
                        c.output(KV.of(c.element().getKey(), v));
                      }
                    }
                  }));
    }
  }

  private static class RegroupOntoSingleKey<V> extends PTransform<PCollection<V>, PCollection<V>> {
    @Override
    public PCollection<V> expand(PCollection<V> input) {
      return input
          .apply(WithKeys.<Void, V>of((Void) null))
          .apply(new Regroup<Void, V>())
          .apply(Values.<V>create());
    }
  }
}
