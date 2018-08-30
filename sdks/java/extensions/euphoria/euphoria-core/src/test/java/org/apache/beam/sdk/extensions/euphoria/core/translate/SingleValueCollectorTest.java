package org.apache.beam.sdk.extensions.euphoria.core.translate;

import java.util.Map;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SingleJvmAccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SingleJvmAccumulatorProvider.Factory;
import org.apache.beam.sdk.extensions.euphoria.core.util.Settings;
import org.junit.Assert;
import org.junit.Test;

/** {@link SingleValueCollector} unit tests. */
public class SingleValueCollectorTest {

  private static final String TEST_COUNTER_NAME = "test-counter";
  private static final String TEST_HISTOGRAM_NAME = "test-histogram";

  @Test
  public void testBasicAccumulatorsAccess() {

    final AccumulatorProvider accumulators =
        SingleJvmAccumulatorProvider.Factory.get().create(new Settings());

    SingleValueCollector collector = new SingleValueCollector(accumulators, "test-no_op_name");

    Counter counter = collector.getCounter(TEST_COUNTER_NAME);
    Assert.assertNotNull(counter);

    Histogram histogram = collector.getHistogram(TEST_HISTOGRAM_NAME);
    Assert.assertNotNull(histogram);

    // collector.getTimer() <- not yet supported
  }

  @Test
  public void testBasicAccumulatorsFunction() {

    Factory accFactory = Factory.get();
    final AccumulatorProvider accumulators = accFactory.create(new Settings());

    SingleValueCollector collector = new SingleValueCollector(accumulators, "test-no_op_name");

    Counter counter = collector.getCounter(TEST_COUNTER_NAME);
    Assert.assertNotNull(counter);

    counter.increment();
    counter.increment(2);

    Map<String, Long> counterSnapshots = accFactory.getCounterSnapshots();
    long counteValue = counterSnapshots.get(TEST_COUNTER_NAME);
    Assert.assertEquals(3L, counteValue);

    Histogram histogram = collector.getHistogram(TEST_HISTOGRAM_NAME);
    Assert.assertNotNull(histogram);

    histogram.add(1);
    histogram.add(2, 2);

    Map<String, Map<Long, Long>> histogramSnapshots = accFactory.getHistogramSnapshots();
    Map<Long, Long> histogramValue = histogramSnapshots.get(TEST_HISTOGRAM_NAME);

    long numOfValuesOfOne = histogramValue.get(1L);
    Assert.assertEquals(1L, numOfValuesOfOne);
    long numOfValuesOfTwo = histogramValue.get(2L);
    Assert.assertEquals(2L, numOfValuesOfTwo);

    // collector.getTimer() <- not yet supported
  }
}
