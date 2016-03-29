/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk;

import com.google.protobuf.ByteString;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.io.Serializable;

/**
 * Matchers that are useful when writing Dataflow tests.
 */
public class DataflowMatchers {
  /**
   * Matcher for {@link ByteString} that prints the strings in UTF8.
   */
  public static class ByteStringMatcher extends TypeSafeMatcher<ByteString>
      implements Serializable {
    private ByteString expected;
    private ByteStringMatcher(ByteString expected) {
      this.expected = expected;
    }

    public static ByteStringMatcher byteStringEq(ByteString expected) {
      return new ByteStringMatcher(expected);
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("ByteString(")
          .appendText(expected.toStringUtf8())
          .appendText(")");
    }

    @Override
    public void describeMismatchSafely(ByteString actual, Description description) {
      description
          .appendText("was ByteString(")
          .appendText(actual.toStringUtf8())
          .appendText(")");
    }

    @Override
    protected boolean matchesSafely(ByteString actual) {
      return actual.equals(expected);
    }
  }
}
