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
package org.apache.beam.sdk.util.common;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ReflectHelpers}. */
@RunWith(JUnit4.class)
public class ReflectHelpersTest {

  @Test
  public void testClassName() {
    assertEquals(getClass().getName(), ReflectHelpers.CLASS_NAME.apply(getClass()));
  }

  @Test
  public void testClassSimpleName() {
    assertEquals(getClass().getSimpleName(), ReflectHelpers.CLASS_SIMPLE_NAME.apply(getClass()));
  }

  @Test
  public void testMethodFormatter() throws Exception {
    assertEquals(
        "testMethodFormatter()",
        ReflectHelpers.METHOD_FORMATTER.apply(getClass().getMethod("testMethodFormatter")));

    assertEquals(
        "oneArg(int)",
        ReflectHelpers.METHOD_FORMATTER.apply(getClass().getDeclaredMethod("oneArg", int.class)));
    assertEquals(
        "twoArg(String, List)",
        ReflectHelpers.METHOD_FORMATTER.apply(
            getClass().getDeclaredMethod("twoArg", String.class, List.class)));
  }

  @Test
  public void testClassMethodFormatter() throws Exception {
    assertEquals(
        getClass().getName() + "#testMethodFormatter()",
        ReflectHelpers.CLASS_AND_METHOD_FORMATTER.apply(
            getClass().getMethod("testMethodFormatter")));

    assertEquals(
        getClass().getName() + "#oneArg(int)",
        ReflectHelpers.CLASS_AND_METHOD_FORMATTER.apply(
            getClass().getDeclaredMethod("oneArg", int.class)));
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
    assertEquals("Integer", ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(Integer.class));
    assertEquals("int", ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(int.class));
    assertEquals("Map", ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(Map.class));
    assertEquals(
        getClass().getSimpleName(), ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(getClass()));
  }

  @Test
  public void testTypeFormatterOnArrays() throws Exception {
    assertEquals("Integer[]", ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(Integer[].class));
    assertEquals("int[]", ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(int[].class));
  }

  @Test
  public void testTypeFormatterWithGenerics() throws Exception {
    assertEquals(
        "Map<Integer, String>",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(
            new TypeDescriptor<Map<Integer, String>>() {}.getType()));
    assertEquals(
        "Map<?, String>",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(
            new TypeDescriptor<Map<?, String>>() {}.getType()));
    assertEquals(
        "Map<? extends Integer, String>",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(
            new TypeDescriptor<Map<? extends Integer, String>>() {}.getType()));
  }

  @Test
  public <T> void testTypeFormatterWithWildcards() throws Exception {
    assertEquals(
        "Map<T, T>",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(new TypeDescriptor<Map<T, T>>() {}.getType()));
  }

  @Test
  public <InputT, OutputT> void testTypeFormatterWithMultipleWildcards() throws Exception {
    assertEquals(
        "Map<? super InputT, ? extends OutputT>",
        ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(
            new TypeDescriptor<Map<? super InputT, ? extends OutputT>>() {}.getType()));
  }

  /** Test interface. */
  public interface Options extends PipelineOptions {
    @Default.String("package.OuterClass$InnerClass#method()")
    String getString();

    @JsonIgnore
    Object getObject();
  }

  @Test
  public void testAnnotationFormatter() throws Exception {
    assertEquals(
        "Default.String(value=package.OuterClass$InnerClass#method())",
        ReflectHelpers.ANNOTATION_FORMATTER.apply(
            Options.class.getMethod("getString").getAnnotations()[0]));

    assertEquals(
        "JsonIgnore(value=true)",
        ReflectHelpers.ANNOTATION_FORMATTER.apply(
            Options.class.getMethod("getObject").getAnnotations()[0]));
  }

  @Test
  public void testFindProperClassLoaderIfContextClassLoaderIsNull() throws InterruptedException {
    final ClassLoader[] classLoader = new ClassLoader[1];
    Thread thread = new Thread(() -> classLoader[0] = ReflectHelpers.findClassLoader());
    thread.setContextClassLoader(null);
    thread.start();
    thread.join();
    assertEquals(ReflectHelpers.class.getClassLoader(), classLoader[0]);
  }

  @Test
  public void testFindProperClassLoaderIfContextClassLoaderIsAvailable()
      throws InterruptedException {
    final ClassLoader[] classLoader = new ClassLoader[1];
    Thread thread = new Thread(() -> classLoader[0] = ReflectHelpers.findClassLoader());
    ClassLoader cl = new ClassLoader() {};
    thread.setContextClassLoader(cl);
    thread.start();
    thread.join();
    assertEquals(cl, classLoader[0]);
  }

  /**
   * Test service interface and implementations for loadServicesOrdered.
   *
   * <p>Note that rather than using AutoService to create resources, AlphaImpl and ZetaImpl are
   * listed in reverse lexicographical order in
   * sdks/java/core/src/test/resources/META-INF/services/org.apache.beam.sdk.util.common.ReflectHelpersTest$FakeService
   * so that we can verify loadServicesOrdered properly re-orders them.
   */
  public interface FakeService {
    String getName();
  }

  /** Alpha implemnetation of FakeService. Should be loaded first */
  public static class AlphaImpl implements FakeService {
    @Override
    public String getName() {
      return "Alpha";
    }
  }

  /** Zeta implemnetation of FakeService. Should be loaded second */
  public static class ZetaImpl implements FakeService {
    @Override
    public String getName() {
      return "Zeta";
    }
  }

  @Test
  public void testLoadServicesOrderedReordersClassesByName() {
    List<String> names = new ArrayList<>();
    for (FakeService service : ReflectHelpers.loadServicesOrdered(FakeService.class)) {
      names.add(service.getName());
    }

    assertThat(names, contains("Alpha", "Zeta"));
  }
}
