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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Set;

/** Tests for {@link ProxyInvocationHandler}. */
@RunWith(JUnit4.class)
public class ValueProviderTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  /** A test interface with some primitives and objects. */
  public static interface Simple extends PipelineOptions {
    boolean isOptionEnabled();
    void setOptionEnabled(boolean value);
    int getPrimitive();
    void setPrimitive(int value);
    String getString();
    void setString(String value);
  }

  @Test
  public void testStaticValueProvider() {
    assertEquals("foo", "foo");
  }
}
