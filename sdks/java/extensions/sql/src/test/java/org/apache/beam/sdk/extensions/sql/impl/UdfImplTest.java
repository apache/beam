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
package org.apache.beam.sdk.extensions.sql.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Method;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link UdfImpl}. */
@RunWith(JUnit4.class)
public class UdfImplTest {

  @Test
  public void testFindMethod_overloaded_prioritizesMaxParams() {
    Method method = UdfImpl.findMethod(OverloadedFn.class, "eval");
    assertNotNull(method);
    assertEquals(3, method.getParameterTypes().length);
  }

  @Test
  public void testFindMethod_singleMethod() {
    Method method = UdfImpl.findMethod(SingleFn.class, "eval");
    assertNotNull(method);
    assertEquals(1, method.getParameterTypes().length);
  }

  public static class OverloadedFn {
    public String eval(String a) {
      return a;
    }

    public String eval(String a, String b) {
      return a + b;
    }

    public String eval(String a, String b, String c) {
      return a + b + c;
    }
  }

  public static class SingleFn {
    public String eval(String a) {
      return a;
    }
  }
}
