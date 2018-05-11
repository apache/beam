/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** TODO: complete javadoc. */
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
