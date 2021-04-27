package org.apache.beam.sdk.io.gcp.datastore;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.DoFnTester.CloningBehavior;
import org.apache.beam.sdk.util.Sleeper;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link RampupThrottlingFn}.
 */
@RunWith(JUnit4.class)
public class RampupThrottlingFnTest {

  private Sleeper mockSleeper = millis -> {
    throw new RampupDelayException();
  };
  private DoFnTester<Void, Void> rampupThrottlingFnTester;

  @Before
  public void setUp() throws InterruptedException {
    MockitoAnnotations.openMocks(this);

    DateTimeUtils.setCurrentMillisFixed(0);
    RampupThrottlingFn<Void> rampupThrottlingFn = new RampupThrottlingFn<>(1);
    rampupThrottlingFn.sleeper = mockSleeper;
    rampupThrottlingFnTester = DoFnTester.of(rampupThrottlingFn);
    rampupThrottlingFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
  }

  @Test
  public void testRampupThrottler() throws Exception {
    Map<Duration, Integer> rampupSchedule = ImmutableMap.<Duration, Integer>builder()
        .put(Duration.ZERO, 500)
        .put(Duration.millis(1), 0)
        .put(Duration.standardSeconds(1), 500)
        .put(Duration.standardSeconds(1).plus(Duration.millis(1)), 0)
        .put(Duration.standardMinutes(5), 500)
        .put(Duration.standardMinutes(10), 750)
        .put(Duration.standardMinutes(15), 1125)
        .put(Duration.standardMinutes(30), 3796)
        .put(Duration.standardMinutes(60), 43248)
        .build();

    for (Map.Entry<Duration, Integer> entry : rampupSchedule.entrySet()) {
      DateTimeUtils.setCurrentMillisFixed(entry.getKey().getMillis());
      for (int i = 0; i < entry.getValue(); i++) {
        rampupThrottlingFnTester.processElement(null);
      }
      assertThrows(RampupDelayException.class, () -> rampupThrottlingFnTester.processElement(null));
    }
  }

  static class RampupDelayException extends InterruptedException {

  }

}
