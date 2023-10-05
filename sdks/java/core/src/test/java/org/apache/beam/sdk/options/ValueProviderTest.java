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
package org.apache.beam.sdk.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.RuntimeValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ValueProvider}. */
@RunWith(JUnit4.class)
public class ValueProviderTest {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));
  @Rule public ExpectedException expectedException = ExpectedException.none();

  /** A test interface. */
  public interface TestOptions extends PipelineOptions {
    @Default.String("bar")
    ValueProvider<String> getBar();

    void setBar(ValueProvider<String> bar);

    ValueProvider<String> getFoo();

    void setFoo(ValueProvider<String> foo);

    ValueProvider<List<Integer>> getList();

    void setList(ValueProvider<List<Integer>> list);
  }

  @Test
  public void testCommandLineNoDefault() {
    TestOptions options = PipelineOptionsFactory.fromArgs("--foo=baz").as(TestOptions.class);
    ValueProvider<String> provider = options.getFoo();
    assertEquals("baz", provider.get());
    assertTrue(provider.isAccessible());
  }

  @Test
  public void testListValueProvider() {
    TestOptions options = PipelineOptionsFactory.fromArgs("--list=1,2,3").as(TestOptions.class);
    ValueProvider<List<Integer>> provider = options.getList();
    assertEquals(ImmutableList.of(1, 2, 3), provider.get());
    assertTrue(provider.isAccessible());
  }

  @Test
  public void testCommandLineWithDefault() {
    TestOptions options = PipelineOptionsFactory.fromArgs("--bar=baz").as(TestOptions.class);
    ValueProvider<String> provider = options.getBar();
    assertEquals("baz", provider.get());
    assertTrue(provider.isAccessible());
  }

  @Test
  public void testStaticValueProvider() {
    ValueProvider<String> provider = StaticValueProvider.of("foo");
    assertEquals("foo", provider.get());
    assertTrue(provider.isAccessible());
    assertEquals("foo", provider.toString());
    assertEquals(provider, StaticValueProvider.of("foo"));
  }

  @Test
  public void testNoDefaultRuntimeProvider() {
    TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
    ValueProvider<String> provider = options.getFoo();
    assertFalse(provider.isAccessible());

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Value only available at runtime");
    expectedException.expectMessage("foo");
    provider.get();
  }

  @Test
  public void testRuntimePropertyName() {
    TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
    ValueProvider<String> provider = options.getFoo();
    assertEquals("foo", ((RuntimeValueProvider) provider).propertyName());
    assertEquals("RuntimeValueProvider{propertyName=foo, default=null}", provider.toString());
  }

  @Test
  public void testDefaultRuntimeProvider() {
    TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
    ValueProvider<String> provider = options.getBar();
    assertFalse(provider.isAccessible());
    assertEquals(provider, options.getBar());
  }

  @Test
  public void testNoDefaultRuntimeProviderWithOverride() throws Exception {
    TestOptions runtime =
        MAPPER
            .readValue("{ \"options\": { \"foo\": \"quux\" }}", PipelineOptions.class)
            .as(TestOptions.class);

    TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
    runtime.setOptionsId(options.getOptionsId());
    RuntimeValueProvider.setRuntimeOptions(runtime);

    ValueProvider<String> provider = options.getFoo();
    assertTrue(provider.isAccessible());
    assertEquals("quux", provider.get());
  }

  @Test
  public void testDefaultRuntimeProviderWithOverride() throws Exception {
    TestOptions runtime =
        MAPPER
            .readValue("{ \"options\": { \"bar\": \"quux\" }}", PipelineOptions.class)
            .as(TestOptions.class);

    TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
    runtime.setOptionsId(options.getOptionsId());
    RuntimeValueProvider.setRuntimeOptions(runtime);

    ValueProvider<String> provider = options.getBar();
    assertTrue(provider.isAccessible());
    assertEquals("quux", provider.get());
  }

  @Test
  public void testDefaultRuntimeProviderWithoutOverride() throws Exception {
    TestOptions runtime = PipelineOptionsFactory.as(TestOptions.class);
    TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
    runtime.setOptionsId(options.getOptionsId());
    RuntimeValueProvider.setRuntimeOptions(runtime);

    ValueProvider<String> provider = options.getBar();
    assertTrue(provider.isAccessible());
    assertEquals("bar", provider.get());
  }

  /** A test interface. */
  public interface BadOptionsRuntime extends PipelineOptions {
    RuntimeValueProvider<String> getBar();

    void setBar(RuntimeValueProvider<String> bar);
  }

  @Test
  public void testOptionReturnTypeRuntime() {
    BadOptionsRuntime options = PipelineOptionsFactory.as(BadOptionsRuntime.class);
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "Method getBar should not have return type "
            + "RuntimeValueProvider, use ValueProvider instead.");
    options.getBar();
  }

  /** A test interface. */
  public interface BadOptionsStatic extends PipelineOptions {
    StaticValueProvider<String> getBar();

    void setBar(StaticValueProvider<String> bar);
  }

  @Test
  public void testOptionReturnTypeStatic() {
    BadOptionsStatic options = PipelineOptionsFactory.as(BadOptionsStatic.class);
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "Method getBar should not have return type "
            + "StaticValueProvider, use ValueProvider instead.");
    options.getBar();
  }

  @Test
  public void testSerializeDeserializeNoArg() throws Exception {
    TestOptions submitOptions = PipelineOptionsFactory.as(TestOptions.class);
    assertFalse(submitOptions.getFoo().isAccessible());

    ObjectNode root = MAPPER.valueToTree(submitOptions);
    ((ObjectNode) root.get("options")).put("foo", "quux");
    TestOptions runtime = MAPPER.convertValue(root, PipelineOptions.class).as(TestOptions.class);

    ValueProvider<String> vp = runtime.getFoo();
    assertTrue(vp.isAccessible());
    assertEquals("quux", vp.get());
    assertEquals(vp.getClass(), StaticValueProvider.class);
  }

  @Test
  public void testSerializeDeserializeWithArg() throws Exception {
    TestOptions submitOptions = PipelineOptionsFactory.fromArgs("--foo=baz").as(TestOptions.class);
    assertTrue(submitOptions.getFoo().isAccessible());
    assertEquals("baz", submitOptions.getFoo().get());

    ObjectNode root = MAPPER.valueToTree(submitOptions);
    ((ObjectNode) root.get("options")).put("foo", "quux");
    TestOptions runtime = MAPPER.convertValue(root, PipelineOptions.class).as(TestOptions.class);

    ValueProvider<String> vp = runtime.getFoo();
    assertTrue(vp.isAccessible());
    assertEquals("quux", vp.get());
  }

  @Test
  public void testNestedValueProviderStatic() throws Exception {
    SerializableFunction<String, String> function = from -> from + "bar";
    ValueProvider<String> svp = StaticValueProvider.of("foo");
    ValueProvider<String> nvp = NestedValueProvider.of(svp, function);
    assertTrue(nvp.isAccessible());
    assertEquals("foobar", nvp.get());
    assertEquals("foobar", nvp.toString());
    assertEquals(nvp, NestedValueProvider.of(svp, function));
  }

  @Test
  public void testNestedValueProviderRuntime() throws Exception {
    TestOptions options = PipelineOptionsFactory.as(TestOptions.class);
    ValueProvider<String> rvp = options.getBar();
    ValueProvider<String> nvp = NestedValueProvider.of(rvp, from -> from + "bar");
    ValueProvider<String> doubleNvp = NestedValueProvider.of(nvp, from -> from);
    assertEquals("bar", ((NestedValueProvider) nvp).propertyName());
    assertEquals("bar", ((NestedValueProvider) doubleNvp).propertyName());
    assertFalse(nvp.isAccessible());
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Value only available at runtime");
    nvp.get();
  }

  private static class NonSerializable {}

  private static class NonSerializableTranslator
      implements SerializableFunction<String, NonSerializable> {
    @Override
    public NonSerializable apply(String from) {
      return new NonSerializable();
    }
  }

  @Test
  public void testNestedValueProviderSerialize() throws Exception {
    ValueProvider<NonSerializable> nvp =
        NestedValueProvider.of(StaticValueProvider.of("foo"), new NonSerializableTranslator());
    SerializableUtils.ensureSerializable(nvp);
  }

  private static class IncrementAtomicIntegerTranslator
      implements SerializableFunction<AtomicInteger, Integer> {
    @Override
    public Integer apply(AtomicInteger from) {
      return from.incrementAndGet();
    }
  }

  @Test
  public void testNestedValueProviderCached() throws Exception {
    AtomicInteger increment = new AtomicInteger();
    ValueProvider<Integer> nvp =
        NestedValueProvider.of(
            StaticValueProvider.of(increment), new IncrementAtomicIntegerTranslator());
    Integer originalValue = nvp.get();
    Integer cachedValue = nvp.get();
    Integer incrementValue = increment.incrementAndGet();
    Integer secondCachedValue = nvp.get();
    assertEquals(originalValue, cachedValue);
    assertEquals(secondCachedValue, cachedValue);
    assertNotEquals(originalValue, incrementValue);
  }
}
