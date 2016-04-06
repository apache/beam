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
package com.google.cloud.dataflow.sdk.util.common;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link ReflectHelpers}.
 */
@RunWith(JUnit4.class)
public class ReflectHelpersTest {

  @Test
  public void testClassName() {
    assertEquals(getClass().getName(), ReflectHelpers.CLASS_NAME.apply(getClass()));
  }

  @Test
  public void testClassSimpleName() {
    assertEquals(getClass().getSimpleName(),
        ReflectHelpers.CLASS_SIMPLE_NAME.apply(getClass()));
  }

  @Test
  public void testMethodFormatter() throws Exception {
    assertEquals("testMethodFormatter()",
        ReflectHelpers.METHOD_FORMATTER.apply(getClass().getMethod("testMethodFormatter")));

    assertEquals("oneArg(int)",
        ReflectHelpers.METHOD_FORMATTER.apply(getClass().getDeclaredMethod("oneArg", int.class)));
    assertEquals("twoArg(String, List)",
        ReflectHelpers.METHOD_FORMATTER.apply(
            getClass().getDeclaredMethod("twoArg", String.class, List.class)));
  }

  @Test
  public void testClassMethodFormatter() throws Exception {
    assertEquals(
        getClass().getName() + "#testMethodFormatter()",
        ReflectHelpers.CLASS_AND_METHOD_FORMATTER
        .apply(getClass().getMethod("testMethodFormatter")));

    assertEquals(
        getClass().getName() + "#oneArg(int)",
        ReflectHelpers.CLASS_AND_METHOD_FORMATTER
        .apply(getClass().getDeclaredMethod("oneArg", int.class)));
    assertEquals(
        getClass().getName() + "#twoArg(String, List)",
        ReflectHelpers.CLASS_AND_METHOD_FORMATTER.apply(
            getClass().getDeclaredMethod("twoArg", String.class, List.class)));
  }

  @SuppressWarnings("unused")
  void oneArg(int n) {}
  @SuppressWarnings("unused")
  void twoArg(String foo, List<Integer> bar) {}

  @Test
  public void testTypeFormatterOnClasses() throws Exception {
    assertEquals("Integer",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(Integer.class));
    assertEquals("int",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(int.class));
    assertEquals("Map",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(Map.class));
    assertEquals(getClass().getSimpleName(),
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(getClass()));
  }

  @Test
  public void testTypeFormatterOnArrays() throws Exception {
    assertEquals("Integer[]",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(Integer[].class));
    assertEquals("int[]",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(int[].class));
  }

  @Test
  public void testTypeFormatterWithGenerics() throws Exception {
    assertEquals("Map<Integer, String>",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(
            new TypeDescriptor<Map<Integer, String>>() {}.getType()));
    assertEquals("Map<?, String>",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(
            new TypeDescriptor<Map<?, String>>() {}.getType()));
    assertEquals("Map<? extends Integer, String>",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(
            new TypeDescriptor<Map<? extends Integer, String>>() {}.getType()));
  }

  @Test
  public <T> void testTypeFormatterWithWildcards() throws Exception {
    assertEquals("Map<T, T>",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(
            new TypeDescriptor<Map<T, T>>() {}.getType()));
  }

  @Test
  public <InputT, OutputT> void testTypeFormatterWithMultipleWildcards() throws Exception {
    assertEquals("Map<? super InputT, ? extends OutputT>",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(
            new TypeDescriptor<Map<? super InputT, ? extends OutputT>>() {}.getType()));
  }
}
