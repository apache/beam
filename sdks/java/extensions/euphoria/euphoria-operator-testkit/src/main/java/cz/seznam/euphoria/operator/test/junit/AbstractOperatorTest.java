/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.operator.test.junit.Processing.Type;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Preconditions;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public abstract class AbstractOperatorTest implements Serializable {

  /**
   * Automatically injected if the test class or the a suite it is part of
   * is annotated with {@code @RunWith(ExecutorProviderRunner.class)}.
   */
  protected transient Executor executor;
  
  protected transient Processing.Type processing;
  
  /**
   * A single test case.
   */
  protected interface TestCase<T> extends Serializable {

    /**
     * @return the number of output partitions to expect in the test output
     */
    int getNumOutputPartitions();

    /**
     * Retrieve flow to be run. Write outputs to given sink.
     *
     * @param flow the flow to attach the test logic to
     * @param bounded if the test is constructed for bounded inputs/outputs
     *
     * @return the output data set representing the result of the test logic
     */
    Dataset<T> getOutput(Flow flow, boolean bounded);

    /**
     * Validate outputs.
     *
     * @param partitions the partitions to be validated representing the output
     *         of the test logic
     */
    void validate(Partitions<T> partitions);

    /** @return the number of runs for the test */
    default int getNumRuns() { return 1; }
  }

  /**
   * Abstract {@code TestCase} to be extended by test classes.
   */
  public static abstract class AbstractTestCase<I, O> implements TestCase<O> {

    final protected Flow flow;
    final protected Settings settings;

    private static String getCallerName() {
      StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
      if (stackTrace.length > 4) {
        return stackTrace[4].getMethodName();
      }
      return "UNKNOWN";
    }

    protected AbstractTestCase() {
      this(getCallerName());
    }

    protected AbstractTestCase(Settings settings) {
      this(getCallerName(), settings);
    }

    protected AbstractTestCase(String name) {
      this(name, new Settings());
    }

    protected AbstractTestCase(String name, Settings settings) {
      this.flow = Flow.create(name, settings);
      this.settings = settings;
    }

    @Override
    public final Dataset<O> getOutput(Flow flow, boolean bounded) {
      Partitions<I> inputData = getInput();
      DataSource<I> dataSource = inputData.asListDataSource(bounded);
      Dataset<I> inputDataset = flow.createInput(dataSource);
      Dataset<O> output = getOutput(inputDataset);
      return output;
    }
    
    protected abstract Dataset<O> getOutput(Dataset<I> input);

    protected abstract Partitions<I> getInput();
  }
  
  public static class Partitions<T> {
    
    private final ArrayList<List<T>> data;
    private final Duration readDelay;
    private final Duration finalDelay;
    
    private Partitions(ArrayList<List<T>> data) {
      this(data, Duration.ofMillis(0), Duration.ofMillis(0));
    }
    
    private Partitions(ArrayList<List<T>> data, Duration readDelay, Duration finalDelay) {
      this.data = Objects.requireNonNull(data);
      this.readDelay = Objects.requireNonNull(readDelay);
      this.finalDelay = Objects.requireNonNull(finalDelay);
    }

    public ListDataSource<T> asListDataSource(boolean bounded) {
      return ListDataSource.of(bounded, data)
          .withReadDelay(readDelay)
          .withFinalDelay(finalDelay);
    }

    @SafeVarargs
    public static <T> Builder<T> add(T ... data) {
      return add(Arrays.asList(data));
    }
    
    public static <T> Builder<T> add(List<T> data) {
      Builder<T> builder = new Builder<>();
      return builder.add(data);
    }
    
    public int size() {
      return data.size();
    }
    
    public List<T> get(int partitionId) {
      return data.get(partitionId);
    }
    
    public List<List<T>> getAll() {
      return data;
    }

    public static class Builder<T> {
      private final ArrayList<List<T>> data = new ArrayList<>();
      private Builder() {}
      public Partitions.Builder<T> add(List<T> data) {
        this.data.add(data);
        return this;
      }
      @SafeVarargs
      public final Partitions.Builder<T> add(T ... data) {
        return add(Arrays.asList(data));
      }
      public Partitions<T> build() {
        return new Partitions<>(data);
      }
      public Partitions<T> build(Duration readDelay, Duration finalDelay) {
        return new Partitions<>(data, readDelay, finalDelay);
      }
    }
  }

  /**
   * Run all tests with given executor.
   *
   * @param tc the test case to execute
   */
  @SuppressWarnings("unchecked")
  public void execute(TestCase tc) {
    Preconditions.checkNotNull(executor);
    Preconditions.checkNotNull(processing);

    // execute tests for each of processing types
    assertEquals(1, processing.asList().size());
    for (Processing.Type proc: processing.asList()) {
      for (int i = 0; i < tc.getNumRuns(); i++) {
        ListDataSink<?> sink = ListDataSink.get(tc.getNumOutputPartitions());
        Flow flow = Flow.create(tc.toString());
        Dataset output = tc.getOutput(flow, proc == Type.BOUNDED);
        // skip if output is not supported for the processing type
        output.persist(sink);
        try {
          executor.submit(flow).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException("Test failure at run #" + i, e);
        }
        Partitions<?> partitions = new Partitions<>(sink.getOutputs());
        tc.validate(partitions);
      }
    }
  }

  protected static <T> void assertUnorderedEquals(
      String message, List<T> first, List<T> second) {
    Map<T, Integer> firstSet = countMap(first);
    Map<T, Integer> secondSet = countMap(second);
    if (message != null) {
      assertEquals(message, firstSet, secondSet);
    } else {
      assertEquals(firstSet, secondSet);
    }
  }

  protected static <T> void assertUnorderedEquals(
      List<T> first, List<T> second) {
    assertUnorderedEquals(null, first, second);
  }

  private static <T> Map<T, Integer> countMap(List<T> list) {
    Map<T, Integer> ret = new HashMap<>();
    list.forEach(e -> {
      Integer current = ret.get(e);
      if (current == null) {
        ret.put(e, 1);
      } else {
        ret.put(e, current + 1);
      }
    });
    return ret;
  }
}
