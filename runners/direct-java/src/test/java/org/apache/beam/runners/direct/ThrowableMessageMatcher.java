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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

class ThrowableMessageMatcher extends BaseMatcher<Throwable> {
  private final Matcher<String> messageMatcher;

  public ThrowableMessageMatcher(String message) {
    this.messageMatcher = equalTo(message);
  }

  @Override
  public boolean matches(Object item) {
    if (!(item instanceof Throwable)) {
      return false;
    }
    Throwable that = (Throwable) item;
    return messageMatcher.matches(that.getMessage());
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("a throwable with a message ").appendDescriptionOf(messageMatcher);
  }
}
