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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ShuffleEntry}. */
@RunWith(JUnit4.class)
public class ShuffleEntryTest {
  private static final ByteString KEY = ByteString.copyFrom(new byte[] {0xA});
  private static final ByteString SKEY = ByteString.copyFrom(new byte[] {0xB});
  private static final ByteString VALUE = ByteString.copyFrom(new byte[] {0xC});

  @Test
  public void accessors() {
    ShuffleEntry entry = new ShuffleEntry(KEY, SKEY, VALUE);
    assertThat(entry.getKey(), equalTo(KEY));
    assertThat(entry.getSecondaryKey(), equalTo(SKEY));
    assertThat(entry.getValue(), equalTo(VALUE));
  }

  @Test
  @SuppressWarnings("SelfEquals")
  public void equalsToItself() {
    ShuffleEntry entry = new ShuffleEntry(KEY, SKEY, VALUE);
    assertEquals(entry, entry);
  }

  @Test
  public void equalsForEqualEntries() {
    ShuffleEntry entry0 = new ShuffleEntry(KEY, SKEY, VALUE);
    ShuffleEntry entry1 = new ShuffleEntry(KEY.concat(ByteString.EMPTY), SKEY, VALUE);

    assertEquals(entry0, entry1);
    assertEquals(entry1, entry0);
    assertEquals(entry0.hashCode(), entry1.hashCode());
  }

  @Test
  public void equalsForEqualNullEntries() {
    ShuffleEntry entry0 = new ShuffleEntry(null, null, null);
    ShuffleEntry entry1 = new ShuffleEntry(null, null, null);

    assertEquals(entry0, entry1);
    assertEquals(entry1, entry0);
    assertEquals(entry0.hashCode(), entry1.hashCode());
  }

  @Test
  public void notEqualsWhenKeysDiffer() {
    final ByteString otherKey = ByteString.copyFrom(new byte[] {0x1});
    ShuffleEntry entry0 = new ShuffleEntry(KEY, SKEY, VALUE);
    ShuffleEntry entry1 = new ShuffleEntry(otherKey, SKEY, VALUE);

    assertFalse(entry0.equals(entry1));
    assertFalse(entry1.equals(entry0));
    assertThat(entry0.hashCode(), not(equalTo(entry1.hashCode())));
  }

  @Test
  public void notEqualsWhenKeysDifferOneNull() {
    ShuffleEntry entry0 = new ShuffleEntry(KEY, SKEY, VALUE);
    ShuffleEntry entry1 = new ShuffleEntry(null, SKEY, VALUE);

    assertFalse(entry0.equals(entry1));
    assertFalse(entry1.equals(entry0));
    assertThat(entry0.hashCode(), not(equalTo(entry1.hashCode())));
  }

  @Test
  public void notEqualsWhenSecondaryKeysDiffer() {
    final ByteString otherSKey = ByteString.copyFrom(new byte[] {0x2});
    ShuffleEntry entry0 = new ShuffleEntry(KEY, SKEY, VALUE);
    ShuffleEntry entry1 = new ShuffleEntry(KEY, otherSKey, VALUE);

    assertFalse(entry0.equals(entry1));
    assertFalse(entry1.equals(entry0));
    assertThat(entry0.hashCode(), not(equalTo(entry1.hashCode())));
  }

  @Test
  public void notEqualsWhenSecondaryKeysDifferOneNull() {
    ShuffleEntry entry0 = new ShuffleEntry(KEY, SKEY, VALUE);
    ShuffleEntry entry1 = new ShuffleEntry(KEY, null, VALUE);

    assertFalse(entry0.equals(entry1));
    assertFalse(entry1.equals(entry0));
    assertThat(entry0.hashCode(), not(equalTo(entry1.hashCode())));
  }

  @Test
  public void notEqualsWhenValuesDiffer() {
    final ByteString otherValue = ByteString.copyFrom(new byte[] {0x2});
    ShuffleEntry entry0 = new ShuffleEntry(KEY, SKEY, VALUE);
    ShuffleEntry entry1 = new ShuffleEntry(KEY, SKEY, otherValue);

    assertFalse(entry0.equals(entry1));
    assertFalse(entry1.equals(entry0));
    assertThat(entry0.hashCode(), not(equalTo(entry1.hashCode())));
  }

  @Test
  public void notEqualsWhenValuesDifferOneNull() {
    ShuffleEntry entry0 = new ShuffleEntry(KEY, SKEY, VALUE);
    ShuffleEntry entry1 = new ShuffleEntry(KEY, SKEY, null);

    assertFalse(entry0.equals(entry1));
    assertFalse(entry1.equals(entry0));
    assertThat(entry0.hashCode(), not(equalTo(entry1.hashCode())));
  }

  @Test
  public void emptyNotTheSameAsNull() {
    final ByteString empty = ByteString.EMPTY;
    ShuffleEntry entry0 = new ShuffleEntry(null, null, null);
    ShuffleEntry entry1 = new ShuffleEntry(empty, empty, empty);

    assertFalse(entry0.equals(entry1));
    assertFalse(entry1.equals(entry0));
    assertThat(entry0.hashCode(), not(equalTo(entry1.hashCode())));
  }
}
