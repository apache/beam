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
package org.apache.beam.sdk.values;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link EncodableThrowable}. */
@RunWith(JUnit4.class)
public final class EncodableThrowableTest {
  @Test
  public void testEquals() {
    IllegalStateException exception =
        new IllegalStateException(
            "Some illegal state",
            new RuntimeException(
                "Some nested exception", new Exception("Deeply nested exception")));

    EncodableThrowable comparable1 = EncodableThrowable.forThrowable(exception);
    EncodableThrowable comparable2 = EncodableThrowable.forThrowable(exception);

    assertEquals(comparable1, comparable1);
    assertEquals(comparable1, comparable2);
  }

  @Test
  public void testEqualsNonComparable() {
    assertNotEquals(EncodableThrowable.forThrowable(new Exception()), new Throwable());
  }

  @Test
  public void testEqualsDifferentUnderlyingTypes() {
    String message = "some message";
    assertNotEquals(
        EncodableThrowable.forThrowable(new RuntimeException(message)),
        EncodableThrowable.forThrowable(new Exception(message)));
  }
}
