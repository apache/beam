package org.apache.beam.runners.flink.metrics;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FlinkMetricContainer.FlinkMeter}.
 */
@RunWith(JUnit4.class)
public class FlinkMeterTest {

  @Test
  public void testFlinkMeter() {
    FlinkMetricContainer.FlinkMeter meter = new FlinkMetricContainer.FlinkMeter();
    assertThat(meter.getCount(), equalTo(0L));

    meter.markEvent();
    assertThat(meter.getCount(), equalTo(1L));

    meter.markEvent(10L);
    assertThat("Updating the meter by delta", meter.getCount(), equalTo(10L));
  }
}
