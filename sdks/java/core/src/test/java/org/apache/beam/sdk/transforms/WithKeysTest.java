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
package org.apache.beam.sdk.transforms;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ExtractKeys transform. */
@RunWith(JUnit4.class)
public class WithKeysTest {
  private static final String[] COLLECTION = new String[] {"a", "aa", "b", "bb", "bbb"};

  private static final List<KV<Integer, String>> WITH_KEYS =
      Arrays.asList(KV.of(1, "a"), KV.of(2, "aa"), KV.of(1, "b"), KV.of(2, "bb"), KV.of(3, "bbb"));

  private static final List<KV<Integer, String>> WITH_CONST_KEYS =
      Arrays.asList(
          KV.of(100, "a"), KV.of(100, "aa"), KV.of(100, "b"), KV.of(100, "bb"), KV.of(100, "bbb"));

  private static final List<KV<Void, String>> WITH_CONST_NULL_KEYS =
      Arrays.asList(
          KV.of((Void) null, "a"),
          KV.of((Void) null, "aa"),
          KV.of((Void) null, "b"),
          KV.of((Void) null, "bb"),
          KV.of((Void) null, "bbb"));

  @Rule public final TestPipeline p = TestPipeline.create();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(NeedsRunner.class)
  public void testExtractKeys() {

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)).withCoder(StringUtf8Coder.of()));

    PCollection<KV<Integer, String>> output = input.apply(WithKeys.of(new LengthAsKey()));
    PAssert.that(output).containsInAnyOrder(WITH_KEYS);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testConstantKeys() {

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)).withCoder(StringUtf8Coder.of()));

    PCollection<KV<Integer, String>> output = input.apply(WithKeys.of(100));
    PAssert.that(output).containsInAnyOrder(WITH_CONST_KEYS);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testConstantVoidKeys() {

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)).withCoder(StringUtf8Coder.of()));

    PCollection<KV<Void, String>> output = input.apply(WithKeys.of((Void) null));
    PAssert.that(output).containsInAnyOrder(WITH_CONST_NULL_KEYS);

    p.run();
  }

  @Test
  public void testWithKeysGetName() {
    assertEquals("WithKeys", WithKeys.<Integer, String>of(100).getName());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWithKeysWithUnneededWithKeyTypeSucceeds() {

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)).withCoder(StringUtf8Coder.of()));

    PCollection<KV<Integer, String>> output =
        input.apply(WithKeys.of(new LengthAsKey()).withKeyType(TypeDescriptor.of(Integer.class)));
    PAssert.that(output).containsInAnyOrder(WITH_KEYS);

    p.run();
  }

  /** Key a value by its length. */
  public static class LengthAsKey implements SerializableFunction<String, Integer> {
    @Override
    public Integer apply(String value) {
      return value.length();
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void withLambdaAndTypeDescriptorShouldSucceed() {

    PCollection<String> values = p.apply(Create.of("1234", "3210", "0", "-12"));
    PCollection<KV<Integer, String>> kvs =
        values.apply(
            WithKeys.of((SerializableFunction<String, Integer>) Integer::valueOf)
                .withKeyType(TypeDescriptor.of(Integer.class)));

    PAssert.that(kvs)
        .containsInAnyOrder(
            KV.of(1234, "1234"), KV.of(0, "0"), KV.of(-12, "-12"), KV.of(3210, "3210"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void withLambdaAndParameterizedTypeDescriptorShouldSucceed() {

    PCollection<String> values = p.apply(Create.of("1234", "3210"));
    PCollection<KV<List<String>, String>> kvs =
        values.apply(
            WithKeys.of((SerializableFunction<String, List<String>>) Collections::singletonList)
                .withKeyType(TypeDescriptors.lists(TypeDescriptors.strings())));

    PAssert.that(kvs)
        .containsInAnyOrder(
            KV.of(Collections.singletonList("1234"), "1234"),
            KV.of(Collections.singletonList("3210"), "3210"));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void withLambdaAndNoTypeDescriptorShouldThrow() {

    PCollection<String> values = p.apply(Create.of("1234", "3210", "0", "-12"));

    values.apply("ApplyKeysWithWithKeys", WithKeys.of(Integer::valueOf));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to return a default Coder for ApplyKeysWithWithKeys");

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testKeySchemaCoderSet() throws NoSuchSchemaException {
    PCollection<KV<Pojo, String>> pCollection =
        p.apply(Create.of(Lists.newArrayList("1", "2", "3")).withType(TypeDescriptors.strings()))
            .apply(
                WithKeys.<Pojo, String>of(v -> new Pojo(1, v))
                    .withKeyType(TypeDescriptor.of(Pojo.class)));

    TypeDescriptor<Pojo> keyType = TypeDescriptor.of(Pojo.class);
    SchemaRegistry schemaRegistry = SchemaRegistry.createDefault();
    SchemaCoder<Pojo> schemaCoder =
        SchemaCoder.of(
            schemaRegistry.getSchema(keyType),
            keyType,
            schemaRegistry.getToRowFunction(keyType),
            schemaRegistry.getFromRowFunction(keyType));
    Coder<KV<Pojo, String>> expectedCoder = KvCoder.of(schemaCoder, StringUtf8Coder.of());
    assertEquals(expectedCoder, pCollection.getCoder());

    p.run();
  }

  @DefaultSchema(JavaBeanSchema.class)
  private static class Pojo {
    private final long num;
    private final String str;

    @SchemaCreate
    public Pojo(long num, String str) {
      this.num = num;
      this.str = str;
    }

    public long getNum() {
      return this.num;
    }

    public String getStr() {
      return this.str;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Pojo)) {
        return false;
      }
      Pojo pojo = (Pojo) o;
      return num == pojo.num && Objects.equals(str, pojo.str);
    }

    @Override
    public int hashCode() {
      return Objects.hash(num, str);
    }
  }
}
