package cz.seznam.euphoria.operator.test.ng.junit;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.operator.test.ng.junit.Processing.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public abstract class AbstractOperatorTest implements Serializable {
  
  private static final Logger LOG = LoggerFactory.getLogger(AbstractOperatorTest.class); 

  /** Automatically injected if the test class or the a suite it is part of
   * is annotated with {@code @RunWith(ExecutorProviderRunner.class)}.
   */
  protected transient Executor executor;
  
  protected transient Processing.Type processing;
  
  /**
   * A single test case.
   */
  protected interface TestCase<T> extends Serializable {

    /** Retrieve number of output partitions to expect in output. */
    int getNumOutputPartitions();

    /** Retrieve flow to be run. Write outputs to given sink. */
    Dataset<T> getOutput(Flow flow, boolean bounded);

    /** Validate outputs. */
    void validate(Partitions<T> partitions);

    /** Retrieve number of runs for the test. */
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
      DataSource<I> dataSource = ListDataSource.of(bounded, inputData.data)
          .withReadDelay(inputData.readDelay)
          .withFinalDelay(inputData.finalDelay);
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
      public Partitions.Builder<T> add(T ... data) {
        return add(Arrays.asList(data));
      }
      public Partitions<T> build() {
        return new Partitions<>(data);
      }
      public Partitions<T> build(Duration readDelay, Duration finalDelay) {
        return new Partitions<T>(data, readDelay, finalDelay);
      }
    }
  }

  /**
   * Run all tests with given executor.
   */
  @SuppressWarnings("unchecked")
  public void execute(TestCase tc) throws Exception {
    if (processing == null) {
      LOG.info("Skipping test {} because of undefined processing type.", tc.getClass());
    } 
    // execute tests for each of processing types
    else for (Processing.Type proc: this.processing.asList()) {
      for (int i = 0; i < tc.getNumRuns(); i++) {
        ListDataSink<?> sink = ListDataSink.get(tc.getNumOutputPartitions());
        Flow flow = Flow.create(tc.toString());
        Dataset output = tc.getOutput(flow, proc == Type.BOUNDED);
        // skip if output is not supported for the processing type
        output.persist(sink);
        executor.submit(flow).get();
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
