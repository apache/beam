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
import static org.junit.Assert.assertThat;

import java.io.UncheckedIOException;
import org.apache.beam.sdk.transforms.Failure.FailureTag;
import org.apache.beam.sdk.values.TupleTag;
import org.hamcrest.Matchers;
import org.junit.Test;

/** Test constraints and internal behavior of {@link Failure}. */
public class FailureTest {

  @Test(expected = IllegalArgumentException.class)
  public void testFailureTagThrowsForErasedType() {
    TupleTag<Failure<Exception, String>> tag = new TupleTag<>();
    FailureTag.of(tag, new Class<?>[] {});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailureTagThrowsForConstructor() {
    TupleTag<Failure<UncheckedIOException, String>> tag =
        new TupleTag<Failure<UncheckedIOException, String>>() {};
    FailureTag.of(tag, new Class<?>[] {});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testThrowsOnDisjointTypes() {
    TupleTag<Failure<RuntimeException, String>> tag =
        new TupleTag<Failure<RuntimeException, String>>() {};
    FailureTag.of(tag, new Class<?>[] {Exception.class});
  }

  @Test
  public void testAllowsNarrowingViaSubtypes() {
    TupleTag<Failure<RuntimeException, String>> tag =
        new TupleTag<Failure<RuntimeException, String>>() {};
    FailureTag.of(tag, new Class<?>[] {IllegalArgumentException.class, NullPointerException.class});
  }

  @Test
  public void testSuppliesDefaultClass() {
    TupleTag<Failure<RuntimeException, String>> tag =
        new TupleTag<Failure<RuntimeException, String>>() {};
    FailureTag<RuntimeException, String> failureTag = FailureTag.of(tag, new Class<?>[] {});
    assertThat(failureTag.exceptionList(), Matchers.contains(RuntimeException.class));
  }

  @Test
  public void testGetExceptionTypeDescriptor() {
    TupleTag<Failure<Exception, String>> tag = new TupleTag<Failure<Exception, String>>() {};
    assertEquals(Exception.class, FailureTag.getExceptionTypeDescriptor(tag).getRawType());
  }
}
