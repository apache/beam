package cz.seznam.euphoria.inmem;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

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

  // test that updates backwards in time have no effect
  @Test
  public void testTimeNonUniformity() {
    VectorClock clock = new VectorClock(2);
    clock.update(1, 1);
    clock.update(3, 0);
    clock.update(4, 1);
    clock.update(2, 1);
    assertEquals(3, clock.getCurrent());
  }

}