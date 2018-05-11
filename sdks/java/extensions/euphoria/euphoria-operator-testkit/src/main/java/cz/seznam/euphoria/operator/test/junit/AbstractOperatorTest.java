/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.operator.test.junit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.operator.test.accumulators.SingleJvmAccumulatorProvider;
import cz.seznam.euphoria.operator.test.accumulators.SnapshotProvider;
import cz.seznam.euphoria.operator.test.junit.Processing.Type;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Base class for test description of a test case.
 */
public abstract class AbstractOperatorTest implements Serializable {

  /**
   * Automatically injected if the test class or the a suite it is part of is annotated with
   * {@code @RunWith(ExecutorProviderRunner.class)}.
   */
  protected transient Executor executor;

  protected transient Processing.Type processing;

  protected static <T> void assertUnorderedEquals(String message, List<T> first, List<T> second) {
    Map<T, Integer> firstSet = countMap(first);
    Map<T, Integer> secondSet = countMap(second);
    if (message != null) {
      assertEquals(message, firstSet, secondSet);
    } else {
      assertEquals(firstSet, secondSet);
    }
  }

  protected static <T> void assertUnorderedEquals(List<T> first, List<T> second) {

    assertUnorderedEquals(null, first, second);
  }

  private static <T> Map<T, Integer> countMap(List<T> list) {
    Map<T, Integer> ret = new HashMap<>();
    list.forEach(
        e -> {
          Integer current = ret.get(e);
          if (current == null) {
            ret.put(e, 1);
          } else {
            ret.put(e, current + 1);
          }
        });
    return ret;
  }

  /**
   * Run all tests with given executor.
   *
   * @param tc the test case to execute
   */
  @SuppressWarnings("unchecked")
  public void execute(TestCase tc) {
    checkNotNull(executor);
    checkNotNull(processing);

    SingleJvmAccumulatorProvider.Factory accs = SingleJvmAccumulatorProvider.Factory.get();
    executor.setAccumulatorProvider(accs);

    // execute tests for each of processing types
    assertEquals(1, processing.asList().size());
    for (Processing.Type proc : processing.asList()) {
      for (int i = 0; i < tc.getNumRuns(); i++) {
        accs.clear();

        ListDataSink<?> sink = ListDataSink.get();
        Flow flow = Flow.create(tc.toString(), tc.getSettings());
        Dataset output = tc.getOutput(flow, proc == Type.BOUNDED);
        // skip if output is not supported for the processing type
        output.persist(tc.modifySink(sink));
        try {
          executor.submit(flow).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException("Test failure at run #" + i, e);
        }
        List outputs = (List) sink.getOutputs();
        tc.validate(outputs);
        tc.validateAccumulators(accs);
      }
    }
  }

  /** A single test case. */
  protected interface TestCase<T> extends Serializable {

    /**
     * Retrieve flow to be run. Write outputs to given sink.
     *
     * @param flow the flow to attach the test logic to
     * @param bounded if the test is constructed for bounded inputs/outputs
     * @return the output data set representing the result of the test logic
     */
    Dataset<T> getOutput(Flow flow, boolean bounded);

    /**
     * Retrieve expected outputs.
     *
     *<p>These outputs will be compared irrespective of order.
     *
     *  @return list of expected outputs that will be compared irrespective of order
     *
     */
    default List<T> getUnorderedOutput() {
      throw new UnsupportedOperationException(
          "Override either `getUnorderedOutput()`, or `validate`");
    }

    /**
     * Validate that the raw output is correct.
     *
     * @param outputs the raw outputs produced by sink
     * @throws AssertionError when the output is not correct
     */
    default void validate(List<T> outputs) throws AssertionError {
      assertUnorderedEquals(getUnorderedOutput(), outputs);
    }

    /**
     * Validate accumulators given a provider capturing the accumulated values.
     *
     * @param snapshots the provider of the accumulated values
     */
    default void validateAccumulators(SnapshotProvider snapshots) {}

    /** @return the number of runs for the test */
    default int getNumRuns() {
      return 1;
    }

    /** @return test specific settings to be applied to the test flow. */
    default Settings getSettings() {
      return new Settings();
    }

    /**
     * Apply additional parameters to output sink.
     *
     * @param sink the original sink to be modified
     * @return modified sink (with extra parameters applied).
     */
    default ListDataSink<T> modifySink(ListDataSink<T> sink) {
      return sink;
    }
  }

  /** Abstract {@code TestCase} to be extended by test classes. */
  public abstract static class AbstractTestCase<InT, OutT> implements TestCase<OutT> {

    protected final Flow flow;
    protected final Settings settings;
    private final int parallel;

    protected AbstractTestCase() {
      this(getCallerName());
    }

    protected AbstractTestCase(int parallel) {
      this(getCallerName(), new Settings(), parallel);
    }

    protected AbstractTestCase(Settings settings) {
      this(getCallerName(), settings);
    }

    protected AbstractTestCase(String name) {
      this(name, new Settings());
    }

    protected AbstractTestCase(String name, Settings settings) {
      this(name, settings, 2);
    }

    protected AbstractTestCase(String name, Settings settings, int parallel) {
      this.flow = Flow.create(name, settings);
      this.settings = settings;
      this.parallel = parallel;
      if (parallel <= 0) {
        throw new IllegalArgumentException("Parallelism has to be at least 1");
      }
    }

    private static String getCallerName() {
      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
      if (stackTrace.length > 4) {
        return stackTrace[4].getMethodName();
      }
      return "UNKNOWN";
    }

    @Override
    public final Dataset<OutT> getOutput(Flow flow, boolean bounded) {
      List<InT> inputData = getInput();
      DataSource<InT> dataSource = asListDataSource(inputData, bounded, parallel);
      Dataset<InT> inputDataset = flow.createInput(dataSource);
      Dataset<OutT> output = getOutput(inputDataset);
      return output;
    }

    protected abstract Dataset<OutT> getOutput(Dataset<InT> input);

    protected abstract List<InT> getInput();

    private DataSource<InT> asListDataSource(List<InT> inputData, boolean bounded, int parallel) {

      final List<List<InT>> splits = new ArrayList<>();
      int part = inputData.size() / parallel;
      int pos = 0;
      for (int i = 0; i < parallel; i++) {
        List<InT> partData = new ArrayList<>();
        int end = i < parallel - 1 ? pos + part : inputData.size();
        while (pos < end) {
          partData.add(inputData.get(pos++));
        }
        splits.add(partData);
      }
      return ListDataSource.of(bounded, splits);
    }
  }
}
