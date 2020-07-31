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
package org.apache.beam.sdk.testing;

import static org.apache.beam.sdk.testing.SerializableMatchers.allOf;
import static org.apache.beam.sdk.testing.SerializableMatchers.anything;
import static org.apache.beam.sdk.testing.SerializableMatchers.containsInAnyOrder;
import static org.apache.beam.sdk.testing.SerializableMatchers.kvWithKey;
import static org.apache.beam.sdk.testing.SerializableMatchers.not;
import static org.junit.Assert.assertThat;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test case for {@link SerializableMatchers}.
 *
 * <p>Since the only new matchers are those for {@link KV}, only those are tested here, to avoid
 * tediously repeating all of hamcrest's tests.
 *
 * <p>A few wrappers of a hamcrest matchers are tested for serializability. Beyond that, the
 * boilerplate that is identical to each is considered thoroughly tested.
 */
@RunWith(JUnit4.class)
public class SerializableMatchersTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAnythingSerializable() throws Exception {
    SerializableUtils.ensureSerializable(anything());
  }

  @Test
  public void testAllOfSerializable() throws Exception {
    SerializableUtils.ensureSerializable(allOf(anything()));
  }

  @Test
  public void testContainsInAnyOrderSerializable() throws Exception {
    assertThat(
        ImmutableList.of(2, 1, 3),
        SerializableUtils.ensureSerializable(containsInAnyOrder(1, 2, 3)));
  }

  @Test
  public void testContainsInAnyOrderNotSerializable() throws Exception {
    assertThat(
        ImmutableList.of(new NotSerializableClass()),
        SerializableUtils.ensureSerializable(
            containsInAnyOrder(new NotSerializableClassCoder(), new NotSerializableClass())));
  }

  @Test
  public void testKvKeyMatcherSerializable() throws Exception {
    assertThat(KV.of("hello", 42L), SerializableUtils.ensureSerializable(kvWithKey("hello")));
  }

  @Test
  public void testKvMatcherBasicSuccess() throws Exception {
    assertThat(KV.of(1, 2), SerializableMatchers.kv(anything(), anything()));
  }

  @Test
  public void testKvMatcherKeyFailure() throws Exception {
    AssertionError exc =
        assertionShouldFail(
            () -> assertThat(KV.of(1, 2), SerializableMatchers.kv(not(anything()), anything())));
    assertThat(exc.getMessage(), Matchers.containsString("key did not match"));
  }

  @Test
  public void testKvMatcherValueFailure() throws Exception {
    AssertionError exc =
        assertionShouldFail(
            () -> assertThat(KV.of(1, 2), SerializableMatchers.kv(anything(), not(anything()))));
    assertThat(exc.getMessage(), Matchers.containsString("value did not match"));
  }

  @Test
  public void testKvMatcherGBKLikeSuccess() throws Exception {
    assertThat(
        KV.of("key", ImmutableList.of(1, 2, 3)),
        SerializableMatchers.<Object, Iterable<Integer>>kv(
            anything(), containsInAnyOrder(3, 2, 1)));
  }

  @Test
  public void testKvMatcherGBKLikeFailure() throws Exception {
    AssertionError exc =
        assertionShouldFail(
            () ->
                assertThat(
                    KV.of("key", ImmutableList.of(1, 2, 3)),
                    SerializableMatchers.<String, Iterable<Integer>>kv(
                        anything(), containsInAnyOrder(1, 2, 3, 4))));
    assertThat(exc.getMessage(), Matchers.containsString("value did not match"));
  }

  private static AssertionError assertionShouldFail(Runnable runnable) {
    try {
      runnable.run();
    } catch (AssertionError expected) {
      return expected;
    }

    throw new AssertionError("Should have failed.");
  }

  private static class NotSerializableClass {
    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof NotSerializableClass;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  private static class NotSerializableClassCoder extends AtomicCoder<NotSerializableClass> {
    @Override
    public void encode(NotSerializableClass value, OutputStream outStream) {}

    @Override
    public NotSerializableClass decode(InputStream inStream) {
      return new NotSerializableClass();
    }
  }
}
