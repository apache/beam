/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/** Unit tests for {@link ZetaSqlIdUtils}. */
public class ZetaSqlIdUtilsTest {

  @Test
  public void testHandlesSimpleIds() {
    List<String> id = Arrays.asList("aaa", "BbB", "zAzzz00");
    assertEquals("aaa.BbB.zAzzz00", ZetaSqlIdUtils.escapeAndJoin(id));
  }

  @Test
  public void testHandlesMixedIds() {
    List<String> id = Arrays.asList("aaa", "Bb---B", "zAzzz00");
    assertEquals("aaa.`Bb---B`.zAzzz00", ZetaSqlIdUtils.escapeAndJoin(id));
  }

  @Test
  public void testHandlesSpecialChars() {
    List<String> id = Arrays.asList("a\\a", "b`b", "c'c", "d\"d", "e?e");
    assertEquals("`a\\\\a`.`b\\`b`.`c\\'c`.`d\\\"d`.`e\\?e`", ZetaSqlIdUtils.escapeAndJoin(id));
  }

  @Test
  public void testHandlesSpecialCharsInOnePart() {
    List<String> id = Arrays.asList("a\\ab`bc'cd\"de?e");
    assertEquals("`a\\\\ab\\`bc\\'cd\\\"de\\?e`", ZetaSqlIdUtils.escapeAndJoin(id));
  }

  @Test
  public void testHandlesWhiteSpaces() {
    List<String> id = Arrays.asList("a\na", "b\tb", "c\rc", "d\fd");
    assertEquals("`a\\na`.`b\\tb`.`c\\rc`.`d\\fd`", ZetaSqlIdUtils.escapeAndJoin(id));
  }

  @Test
  public void testHandlesWhiteSpacesInOnePart() {
    List<String> id = Arrays.asList("a\nab\tbc\rcd\fd");
    assertEquals("`a\\nab\\tbc\\rcd\\fd`", ZetaSqlIdUtils.escapeAndJoin(id));
  }
}
