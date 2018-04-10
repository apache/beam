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

package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link StructuralKey}.
 */
@RunWith(JUnit4.class)
public class StructuralKeyTest {
  @Test
  public void getKeyEqualToOldKey() {
    assertThat(StructuralKey.of(1234, VarIntCoder.of()).getKey(), equalTo(1234));
    assertThat(StructuralKey.of("foobar", StringUtf8Coder.of()).getKey(), equalTo("foobar"));
    assertArrayEquals(StructuralKey.of(new byte[] {2, 9, -22}, ByteArrayCoder.of()).getKey(),
        new byte[] {2, 9, -22});
  }

  @Test
  public void getKeyNotSameInstance() {
    byte[] original = new byte[] {1, 4, 9, 127, -22};
    StructuralKey<byte[]> key = StructuralKey.of(original, ByteArrayCoder.of());

    assertThat(key.getKey(), not(theInstance(original)));
  }

  @Test
  public void emptyKeysNotEqual() {
    StructuralKey<?> empty = StructuralKey.empty();

    assertThat(empty, not(Matchers.equalTo(StructuralKey.empty())));
    assertThat(empty, Matchers.equalTo(empty));
  }

  @Test
  public void objectEqualsTrueKeyEquals() {
    StructuralKey<Integer> original = StructuralKey.of(1234, VarIntCoder.of());
    assertThat(StructuralKey.of(1234, VarIntCoder.of()), equalTo(original));
  }

  @Test
  public void objectsNotEqualEncodingsEqualEquals() {
    byte[] original = new byte[] {1, 4, 9, 127, -22};
    StructuralKey<byte[]> key = StructuralKey.of(original, ByteArrayCoder.of());

    StructuralKey<byte[]> otherKey =
        StructuralKey.of(new byte[] {1, 4, 9, 127, -22}, ByteArrayCoder.of());
    assertThat(key, equalTo(otherKey));
  }

  @Test
  public void notEqualEncodingsEqual() {
    byte[] original = new byte[] {1, 4, 9, 127, -22};
    StructuralKey<byte[]> key = StructuralKey.of(original, ByteArrayCoder.of());

    StructuralKey<byte[]> otherKey =
        StructuralKey.of(new byte[] {9, -128, 22}, ByteArrayCoder.of());
    assertThat(key, not(equalTo(otherKey)));
  }
}
