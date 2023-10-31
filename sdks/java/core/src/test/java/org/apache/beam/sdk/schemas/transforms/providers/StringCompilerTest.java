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
package org.apache.beam.sdk.schemas.transforms.providers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.function.Function;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class StringCompilerTest {

  public static final String SQUARE_SOURCE =
      "import java.util.function.Function;"
          + "public class Square implements Function<Integer, Integer> {"
          + "  public Integer apply(Integer x) { return x * x; }"
          + "}";

  @Test
  public void testGetClass() throws Exception {
    Class<?> clazz = StringCompiler.getClass("Square", SQUARE_SOURCE);
    assertTrue(Function.class.isAssignableFrom(clazz));
    assertEquals("Square", clazz.getSimpleName());
  }

  @Test
  public void testGetInstance() throws Exception {
    Function<Integer, Integer> square =
        (Function<Integer, Integer>) StringCompiler.getInstance("Square", SQUARE_SOURCE);
    assertEquals(4, (int) square.apply(2));
  }

  @Test
  public void testGuessExpressionType() throws Exception {
    assertEquals(
        double.class,
        StringCompiler.guessExpressionType(
            "a+b", ImmutableMap.of("a", int.class, "b", double.class)));
    assertEquals(
        double.class,
        StringCompiler.guessExpressionType(
            "a > 0 ? a : b", ImmutableMap.of("a", int.class, "b", double.class)));
    assertEquals(
        double.class,
        StringCompiler.guessExpressionType("a * Math.random()", ImmutableMap.of("a", int.class)));
    assertEquals(
        int.class,
        StringCompiler.guessExpressionType("(int) a", ImmutableMap.of("a", double.class)));
    assertEquals(
        long.class,
        StringCompiler.guessExpressionType(
            "a.getInt64(\"foo\")+b", ImmutableMap.of("a", Row.class, "b", int.class)));
  }
}
