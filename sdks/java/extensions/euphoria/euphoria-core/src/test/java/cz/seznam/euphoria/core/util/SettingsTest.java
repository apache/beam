package cz.seznam.euphoria.core.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SettingsTest {

  @Test
  public void testNesting() {
    Settings s = new Settings();
    s.setString("foo", "bar");
    s.setInt("zulu", 42);

    Settings n = s.nested("quack");
    n.setString("foo", "nested-bar");
    n.setInt("zulu", 128);

    // ~ verify 's' is unchanged
    assertEquals("bar", s.getString("foo"));
    assertEquals(42, s.getInt("zulu"));

    // ~ verify the values are shared
    assertEquals("nested-bar", s.getString("quack.foo"));
    assertEquals(128, s.getInt("quack.zulu"));

    // ~ verify 'n' sees different values than 's'
    assertEquals("nested-bar", n.getString("foo"));
    assertEquals(128, n.getInt("zulu"));
  }
}