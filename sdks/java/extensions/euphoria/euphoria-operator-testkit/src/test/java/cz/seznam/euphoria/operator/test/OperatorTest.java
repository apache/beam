
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.inmem.InMemFileSystem;
import cz.seznam.euphoria.core.util.Settings;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


/**
 * Operator test base class.
 * All tests should extend this class.
 */
public abstract class OperatorTest implements Serializable {

  /**
   * A single test case.
   */
  protected interface TestCase<T> extends Serializable {

    /** Retrieve number of output partitions to expect in output. */
    int getNumOutputPartitions();

    /** Retrieve flow to be run. Write outputs to given sink. */
    Dataset<T> getOutput(Flow flow);

    /** Validate outputs. */
    void validate(List<List<T>> partitions);

  }

  /**
   * Run all tests with given executor.
   */
  @SuppressWarnings("unchecked")
  public void runTests(Executor executor, Settings settings) throws Exception {
    for (TestCase tc : getTestCases()) {
      Flow flow = Flow.create(tc.toString(), settings);
      tc.getOutput(flow).persist(URI.create("inmem:///tmp/output"));
      executor.waitForCompletion(flow);

      // load output from InMemFileSystem
      List<List> outputs = new ArrayList<>(tc.getNumOutputPartitions());
      for (int i = 0; i < tc.getNumOutputPartitions(); i++) {
        outputs.add(Arrays.asList(
                InMemFileSystem.get().getFile("/tmp/output/" + i).toArray()));
      }
      tc.validate(outputs);
    }
  }

  /**
   * Retrieve test cases to be run.
   */
  protected abstract List<TestCase> getTestCases();

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
