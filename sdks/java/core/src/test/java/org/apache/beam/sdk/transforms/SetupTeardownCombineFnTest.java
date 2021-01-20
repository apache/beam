package org.apache.beam.sdk.transforms;

import com.google.common.collect.Streams;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

@RunWith(JUnit4.class)
public class SetupTeardownCombineFnTest {
    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category({NeedsRunner.class, ValidatesRunner.class})
    public void simpleTestCombineNoLifting() {
        final List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        final SimpleTestCombine simpleCombine = new SimpleTestCombine();
        final PCollection<Integer> pCollection = pipeline.apply(Create.of(ints))
                .apply(Combine.globally(simpleCombine))
                .apply(Window.<Integer>into(new GlobalWindows())
                        .triggering(AfterPane.elementCountAtLeast(1))
                        .discardingFiredPanes());

        PAssert.that(pCollection).containsInAnyOrder(ints.stream().reduce(0, Integer::sum));
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category({NeedsRunner.class, ValidatesRunner.class})
    public void simpleTestCombineWithLifting() {
        final List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        final SimpleTestCombine simpleCombine = new SimpleTestCombine();
        final PCollection<Integer> pCollection = pipeline.apply(Create.of(ints))
                .apply(Combine.globally(simpleCombine));

        PAssert.that(pCollection).containsInAnyOrder(ints.stream().reduce(0, Integer::sum));
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category({NeedsRunner.class, ValidatesRunner.class})
    public void simpleTestCombineWithContextNoLifting() {
        final List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        final SimpleTestCombineWithContext simpleCombine = new SimpleTestCombineWithContext();
        final PCollection<Integer> pCollection = pipeline.apply(Create.of(ints))
                .apply(Combine.globally(simpleCombine));

        PAssert.that(pCollection).containsInAnyOrder(ints.stream().reduce(0, Integer::sum));
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category({NeedsRunner.class, ValidatesRunner.class})
    public void simpleTestCombineWithContextWithLifting() {
        final List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        final SimpleTestCombineWithContext simpleCombine = new SimpleTestCombineWithContext();
        final PCollection<Integer> pCollection = pipeline.apply(Create.of(ints))
                .apply(Combine.globally(simpleCombine))
                .apply(Window.<Integer>into(new GlobalWindows())
                        .triggering(AfterPane.elementCountAtLeast(1))
                        .discardingFiredPanes());

        PAssert.that(pCollection).containsInAnyOrder(ints.stream().reduce(0, Integer::sum));
        pipeline.run().waitUntilFinish();
    }
}

enum State {
    CREATED, SETUP, FINISHED
}

class SimpleTestCombine extends Combine.CombineFn<Integer, Integer, Integer> {
    private final Logger LOG = LoggerFactory.getLogger(SimpleTestCombine.class);

    private State state = State.CREATED;

    public State getState() {
        return State.valueOf(state.name());
    }

    @Override
    public void setup() {
        LOG.info("setUp called with state {}", state);
        if (state == State.SETUP) {
            throw new IllegalStateException("Incorrect state in setUp: " + state);
        } else {
            state = State.SETUP;
        }
    }

    @Override
    public void teardown() {
        LOG.info("tearDown called");
        if (state == State.SETUP) {
            state = State.FINISHED;
            return;
        }
        throw new IllegalStateException("Incorrect state in tearDown: " + state);
    }

    @Override
    public Integer createAccumulator() {
        LOG.info("createAccumulator called");
        checkState("createAccumulator");
        return 0;
    }

    @Override
    public Integer addInput(Integer mutableAccumulator, Integer input) {
        LOG.info("addInput called");
        checkState("addInput");
        return Integer.sum(mutableAccumulator, input);
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators) {
        LOG.info("mergeAccumulators called");
        checkState("mergeAccumulators");
        return Streams.stream(accumulators).reduce(0, Integer::sum);
    }

    @Override
    public Integer extractOutput(Integer accumulator) {
        LOG.info("extractOutput called");
        checkState("extractOutput");
        return accumulator;
    }

    private void checkState(final String method) {
        if (state != State.SETUP) {
            throw new IllegalStateException("Incorrect state in " + method + ", should be SETUP but was " + state);
        }
    }
}

class SimpleTestCombineWithContext extends CombineWithContext.CombineFnWithContext<Integer, Integer, Integer> {
    private final Logger LOG = LoggerFactory.getLogger(SimpleTestCombineWithContext.class);

    private State state = State.CREATED;

    public State getState() {
        return State.valueOf(state.name());
    }

    @Override
    public void setup() {
        LOG.info("setUp called with state {}", state);
        if (state == State.SETUP) {
            throw new IllegalStateException("Incorrect state in setUp: " + state);
        } else {
            state = State.SETUP;
        }
    }

    @Override
    public void teardown() {
        LOG.info("tearDown called");
        if (state == State.SETUP) {
            state = State.FINISHED;
            return;
        }
        throw new IllegalStateException("Incorrect state in tearDown: " + state);
    }

    @Override
    public Integer createAccumulator(CombineWithContext.Context c) {
        LOG.info("createAccumulator called");
        checkState("createAccumulator");
        return 0;
    }

    @Override
    public Integer addInput(Integer mutableAccumulator, Integer input, CombineWithContext.Context c) {
        LOG.info("addInput called");
        checkState("addInput");
        return Integer.sum(mutableAccumulator, input);
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators, CombineWithContext.Context c) {
        LOG.info("mergeAccumulators called");
        checkState("mergeAccumulators");
        return Streams.stream(accumulators).reduce(0, Integer::sum);
    }

    @Override
    public Integer extractOutput(Integer accumulator, CombineWithContext.Context c) {
        LOG.info("extractOutput called");
        checkState("extractOutput");
        return accumulator;
    }

    private void checkState(final String method) {
        if (state != State.SETUP) {
            throw new IllegalStateException("Incorrect state in " + method + ", should be SETUP but was " + state);
        }
    }
}
