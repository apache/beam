package org.apache.beam.runners.jstorm.translation.translator;

import org.apache.beam.runners.jstorm.TestJStormRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertThat;


/**
 * Tests for Combine transforms.
 */
@RunWith(JUnit4.class)
public class CombineTest implements Serializable {
    // This test is Serializable, just so that it's easy to have
    // anonymous inner classes inside the non-static test methods.

    static final List<KV<String, Integer>> TABLE = Arrays.asList(
            KV.of("a", 1),
            KV.of("a", 1),
            KV.of("a", 4),
            KV.of("b", 1),
            KV.of("b", 13)
    );

    static final List<KV<String, Integer>> EMPTY_TABLE = Collections.emptyList();

    PCollection<KV<String, Integer>> createInput(Pipeline p,
                                                 List<KV<String, Integer>> table) {
        return p.apply(Create.of(table).withCoder(
                KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
    }

    private void runTestSimpleCombine(List<KV<String, Integer>> table,
                                      int globalSum,
                                      List<KV<String, String>> perKeyCombines) {
        StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);
        options.setRunner(TestJStormRunner.class);
        options.setLocalMode(true);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<KV<String, Integer>> input = createInput(pipeline, table);

        PCollection<Integer> sum = input
                .apply(Values.<Integer>create())
                .apply(Combine.globally(new SumInts()));

        // Java 8 will infer.
        PCollection<KV<String, String>> sumPerKey = input
                .apply(Combine.<String, Integer, String>perKey(new TestCombineFn()));

        PAssert.that(sum).containsInAnyOrder(globalSum);
        PAssert.that(sumPerKey).containsInAnyOrder(perKeyCombines);

        pipeline.run();
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testSimpleCombine() {
        runTestSimpleCombine(TABLE, 20, Arrays.asList(KV.of("a", "114"), KV.of("b", "113")));
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testFixedWindowsCombine() {
        StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);
        options.setRunner(TestJStormRunner.class);
        options.setLocalMode(true);

        Pipeline pipeline = Pipeline.create(options);
        PCollection<KV<String, Integer>> input =
                pipeline.apply(Create.timestamped(TABLE, Arrays.asList(0L, 1L, 6L, 7L, 8L))
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
                        .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(2))));

        PCollection<Integer> sum = input
                .apply(Values.<Integer>create())
                .apply(Combine.globally(new SumInts()).withoutDefaults());

        PCollection<KV<String, String>> sumPerKey = input
                .apply(Combine.<String, Integer, String>perKey(new TestCombineFn()));

        PAssert.that(sum).containsInAnyOrder(2, 5, 13);
        PAssert.that(sumPerKey).containsInAnyOrder(
                KV.of("a", "11"),
                KV.of("a", "4"),
                KV.of("b", "1"),
                KV.of("b", "13"));
        pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testFixedWindowsCombineWithContext() {
        StormPipelineOptions options = PipelineOptionsFactory.as(StormPipelineOptions.class);
        options.setRunner(TestJStormRunner.class);
        options.setLocalMode(true);

        Pipeline pipeline = Pipeline.create(options);
        PCollection<KV<String, Integer>> perKeyInput =
                pipeline.apply(Create.timestamped(TABLE, Arrays.asList(0L, 1L, 6L, 7L, 8L))
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())))
                        .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.millis(2))));

        PCollection<Integer> globallyInput = perKeyInput.apply(Values.<Integer>create());

        PCollection<Integer> sum = globallyInput
                .apply("Sum", Combine.globally(new SumInts()).withoutDefaults());

        PCollectionView<Integer> globallySumView = sum.apply(View.<Integer>asSingleton());

        PCollection<KV<String, String>> combinePerKeyWithContext =
                perKeyInput.apply(
                        Combine.<String, Integer, String>perKey(new TestCombineFnWithContext(globallySumView))
                                .withSideInputs(Arrays.asList(globallySumView)));

        PCollection<String> combineGloballyWithContext = globallyInput
                .apply(Combine.globally(new TestCombineFnWithContext(globallySumView))
                        .withoutDefaults()
                        .withSideInputs(Arrays.asList(globallySumView)));

        PAssert.that(sum).containsInAnyOrder(2, 5, 13);
        PAssert.that(combinePerKeyWithContext).containsInAnyOrder(
                KV.of("a", "112"),
                KV.of("a", "45"),
                KV.of("b", "15"),
                KV.of("b", "1133"));
        PAssert.that(combineGloballyWithContext).containsInAnyOrder("112", "145", "1133");
        pipeline.run();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Test classes, for different kinds of combining fns.

    /** Example SerializableFunction combiner. */
    public static class SumInts
            implements SerializableFunction<Iterable<Integer>, Integer> {
        @Override
        public Integer apply(Iterable<Integer> input) {
            int sum = 0;
            for (int item : input) {
                sum += item;
            }
            return sum;
        }
    }

    /**
     * A {@link Combine.CombineFn} that results in a sorted list of all characters occurring in the key and
     * the decimal representations of each value.
     */
    public static class TestCombineFn extends Combine.CombineFn<Integer, TestCombineFn.Accumulator, String> {

        // Not serializable.
        static class Accumulator {
            String value;
            public Accumulator(String value) {
                this.value = value;
            }

            public static Coder<Accumulator> getCoder() {
                return new AtomicCoder<Accumulator>() {
                    @Override
                    public void encode(Accumulator accumulator, OutputStream outStream)
                            throws CoderException, IOException {
                        encode(accumulator, outStream, Coder.Context.NESTED);
                    }

                    @Override
                    public void encode(Accumulator accumulator, OutputStream outStream, Coder.Context context)
                            throws CoderException, IOException {
                        StringUtf8Coder.of().encode(accumulator.value, outStream, context);
                    }

                    @Override
                    public Accumulator decode(InputStream inStream) throws CoderException, IOException {
                        return decode(inStream, Coder.Context.NESTED);
                    }

                    @Override
                    public Accumulator decode(InputStream inStream, Coder.Context context)
                            throws CoderException, IOException {
                        return new Accumulator(StringUtf8Coder.of().decode(inStream, context));
                    }
                };
            }
        }

        @Override
        public Coder<Accumulator> getAccumulatorCoder(
                CoderRegistry registry, Coder<Integer> inputCoder) {
            return Accumulator.getCoder();
        }

        @Override
        public Accumulator createAccumulator() {
            return new Accumulator("");
        }

        @Override
        public Accumulator addInput(Accumulator accumulator, Integer value) {
            try {
                return new Accumulator(accumulator.value + String.valueOf(value));
            } finally {
                accumulator.value = "cleared in addInput";
            }
        }

        @Override
        public Accumulator mergeAccumulators(Iterable<Accumulator> accumulators) {
            String all = "";
            for (Accumulator accumulator : accumulators) {
                all += accumulator.value;
                accumulator.value = "cleared in mergeAccumulators";
            }
            return new Accumulator(all);
        }

        @Override
        public String extractOutput(Accumulator accumulator) {
            char[] chars = accumulator.value.toCharArray();
            Arrays.sort(chars);
            return new String(chars);
        }
    }

    /**
     * A {@link CombineWithContext.CombineFnWithContext} that produces a sorted list of all characters occurring in the
     * key and the decimal representations of main and side inputs values.
     */
    public class TestCombineFnWithContext extends CombineWithContext.CombineFnWithContext<Integer, TestCombineFn.Accumulator, String> {
        private final PCollectionView<Integer> view;

        public TestCombineFnWithContext(PCollectionView<Integer> view) {
            this.view = view;
        }

        @Override
        public Coder<TestCombineFn.Accumulator> getAccumulatorCoder(
                CoderRegistry registry, Coder<Integer> inputCoder) {
            return TestCombineFn.Accumulator.getCoder();
        }

        @Override
        public TestCombineFn.Accumulator createAccumulator(CombineWithContext.Context c) {
            return new TestCombineFn.Accumulator(c.sideInput(view).toString());
        }

        @Override
        public TestCombineFn.Accumulator addInput(
                TestCombineFn.Accumulator accumulator, Integer value, CombineWithContext.Context c) {
            try {
                assertThat(accumulator.value, Matchers.startsWith(c.sideInput(view).toString()));
                return new TestCombineFn.Accumulator(accumulator.value + String.valueOf(value));
            } finally {
                accumulator.value = "cleared in addInput";
            }

        }

        @Override
        public TestCombineFn.Accumulator mergeAccumulators(
                Iterable<TestCombineFn.Accumulator> accumulators, CombineWithContext.Context c) {
            String prefix = c.sideInput(view).toString();
            String all = prefix;
            for (TestCombineFn.Accumulator accumulator : accumulators) {
                assertThat(accumulator.value, Matchers.startsWith(prefix));
                all += accumulator.value.substring(prefix.length());
                accumulator.value = "cleared in mergeAccumulators";
            }
            return new TestCombineFn.Accumulator(all);
        }

        @Override
        public String extractOutput(TestCombineFn.Accumulator accumulator, CombineWithContext.Context c) {
            assertThat(accumulator.value, Matchers.startsWith(c.sideInput(view).toString()));
            char[] chars = accumulator.value.toCharArray();
            Arrays.sort(chars);
            return new String(chars);
        }
    }
}