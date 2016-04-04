package cz.seznam.euphoria.core.time;

import org.junit.Test;

import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimeProvidersTest {

  @Test
  public void testFixedTimeProvider() {

    Date fixed = new Date();
    TimeProvider provider = TimeProviders.getFixedTimeInstance(fixed);

    assertTrue("FixedTimeProvider should return fixed date",
            provider.now().equals(fixed));
  }

  @Test
  public void testTimeProvider_Now_WithOffset() {
    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    //set datetime to 2014-09-18 15:50
    c.set(Calendar.YEAR, 2014);
    c.set(Calendar.MONTH, 9);
    c.set(Calendar.DAY_OF_MONTH, 18);
    c.set(Calendar.HOUR_OF_DAY, 15);
    c.set(Calendar.MINUTE, 50);
    c.set(Calendar.SECOND, 0);

    TimeProvider provider = TimeProviders.getFixedTimeInstance(c.getTime());

    long offset = 60 * 60 * 1000; //1 hour
    Calendar today = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    today.setTime(provider.nowOffset(Duration.ofMillis(-offset)));

    //result should be 2014-09-18 14:50
    assertEquals(2014, today.get(Calendar.YEAR));
    assertEquals(9, today.get(Calendar.MONTH));
    assertEquals(18, today.get(Calendar.DAY_OF_MONTH));
    assertEquals(14, today.get(Calendar.HOUR_OF_DAY));
    assertEquals(50, today.get(Calendar.MINUTE));
    assertEquals(0, today.get(Calendar.SECOND));
  }

  @Test
  public void testTimeProvider_Today() {
    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    //set datetime to 2014-09-18 15:50
    c.set(Calendar.YEAR, 2014);
    c.set(Calendar.MONTH, 9);
    c.set(Calendar.DAY_OF_MONTH, 18);
    c.set(Calendar.HOUR_OF_DAY, 15);
    c.set(Calendar.MINUTE, 50);
    c.set(Calendar.SECOND, 0);

    TimeProvider provider = TimeProviders.getFixedTimeInstance(c.getTime());

    Calendar today = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    today.setTime(provider.today());

    //result should be 2014-09-18 00:00
    assertEquals(2014, today.get(Calendar.YEAR));
    assertEquals(9, today.get(Calendar.MONTH));
    assertEquals(18, today.get(Calendar.DAY_OF_MONTH));
    assertEquals(0, today.get(Calendar.HOUR_OF_DAY));
    assertEquals(0, today.get(Calendar.MINUTE));
    assertEquals(0, today.get(Calendar.SECOND));
  }

}