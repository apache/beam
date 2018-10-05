
package cz.seznam.euphoria.core.executor.inmem;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test vector clocks.
 */
public class VectorClockTest {

  @Test
  public void testUpdate() {
    VectorClock clock = new VectorClock(2);
    clock.update(1, 1);
    assertEquals(0, clock.getCurrent());
    clock.update(2, 0);
    assertEquals(1, clock.getCurrent());
    clock.update(3, 0);
    assertEquals(1, clock.getCurrent());
    clock.update(4, 1);
    assertEquals(3, clock.getCurrent());
  }

}
