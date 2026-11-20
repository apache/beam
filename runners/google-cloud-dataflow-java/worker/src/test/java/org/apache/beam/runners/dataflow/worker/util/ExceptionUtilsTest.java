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
package org.apache.beam.runners.dataflow.worker.util;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ExceptionUtils}. */
@RunWith(JUnit4.class)
public class ExceptionUtilsTest {

  private static class CustomExceptionA extends RuntimeException {
    CustomExceptionA(Throwable cause) {
      super(cause);
    }

    CustomExceptionA(String msg) {
      super(msg);
    }
  }

  private static class CustomExceptionB extends RuntimeException {
    CustomExceptionB(Throwable cause) {
      super(cause);
    }

    CustomExceptionB(String msg) {
      super(msg);
    }
  }

  @Test
  public void hasCause_nullThrowable_returnsFalse() {
    assertThat(ExceptionUtils.hasCause(null, CustomExceptionA.class)).isFalse();
    assertThat(ExceptionUtils.hasCause(null, CustomExceptionA.class, CustomExceptionB.class))
        .isFalse();
    assertThat(ExceptionUtils.hasMatchingCause(null, t -> true)).isFalse();
  }

  @Test
  public void hasCause_directMatch_returnsTrue() {
    CustomExceptionA exception = new CustomExceptionA("direct");
    assertThat(ExceptionUtils.hasCause(exception, CustomExceptionA.class)).isTrue();
    assertThat(ExceptionUtils.hasCause(exception, CustomExceptionA.class, CustomExceptionB.class))
        .isTrue();
    assertThat(ExceptionUtils.hasCause(exception, CustomExceptionB.class, CustomExceptionA.class))
        .isTrue();
    assertThat(ExceptionUtils.hasCause(exception, CustomExceptionB.class)).isFalse();
  }

  @Test
  public void hasCause_singleLevelCause_returnsTrue() {
    RuntimeException wrapper = new RuntimeException(new CustomExceptionA("cause"));
    assertThat(ExceptionUtils.hasCause(wrapper, CustomExceptionA.class)).isTrue();
    assertThat(ExceptionUtils.hasCause(wrapper, CustomExceptionA.class, CustomExceptionB.class))
        .isTrue();
    assertThat(ExceptionUtils.hasCause(wrapper, CustomExceptionB.class)).isFalse();
  }

  @Test
  public void hasCause_deeplyNestedCause_returnsTrue() {
    RuntimeException deep =
        new RuntimeException(
            new IllegalArgumentException(new IOException(new CustomExceptionB("deep"))));
    assertThat(ExceptionUtils.hasCause(deep, CustomExceptionB.class)).isTrue();
    assertThat(ExceptionUtils.hasCause(deep, CustomExceptionA.class, CustomExceptionB.class))
        .isTrue();
    assertThat(ExceptionUtils.hasCause(deep, CustomExceptionA.class)).isFalse();
  }

  private static class SelfReferencingException extends RuntimeException {
    @Override
    public synchronized Throwable getCause() {
      return this;
    }
  }

  @Test
  public void hasCause_selfReferencingCause_terminatesWithoutInfiniteLoop() {
    SelfReferencingException selfReferencing = new SelfReferencingException();
    assertThat(ExceptionUtils.hasCause(selfReferencing, CustomExceptionA.class)).isFalse();
  }

  @Test
  public void hasCause_cyclicCausalChain_terminatesWithoutInfiniteLoop() {
    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second", first);
    first.initCause(second);

    assertThat(ExceptionUtils.hasCause(first, CustomExceptionA.class)).isFalse();
    assertThat(ExceptionUtils.hasCause(first, CustomExceptionA.class, CustomExceptionB.class))
        .isFalse();
  }

  @Test
  public void hasCause_cyclicCausalChainWithMatch_returnsTrue() {
    CustomExceptionA matching = new CustomExceptionA("matching");
    RuntimeException outer = new RuntimeException("outer", matching);
    matching.initCause(outer);

    assertThat(ExceptionUtils.hasCause(outer, CustomExceptionA.class)).isTrue();
  }

  @Test
  public void hasMatchingCause_customPredicate() {
    RuntimeException exception = new RuntimeException("message containing secret_needle");
    assertThat(
            ExceptionUtils.hasMatchingCause(
                exception, t -> t.getMessage().contains("secret_needle")))
        .isTrue();
    assertThat(
            ExceptionUtils.hasMatchingCause(
                exception, t -> t.getMessage().contains("other_needle")))
        .isFalse();
  }
}
