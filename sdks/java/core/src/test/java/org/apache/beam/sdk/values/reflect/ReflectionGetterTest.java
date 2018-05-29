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

package org.apache.beam.sdk.values.reflect;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;
import org.junit.Test;

/**
 * Unit tests for {@link ReflectionGetter}.
 */
public class ReflectionGetterTest {

  /**
   * Test pojo.
   */
  private static class Pojo {
    public String getStringField() {
      return "test";
    }

    public Integer getIntField() {
      return 3421;
    }

    public Integer notGetter() {
      return 542;
    }
  }

  private static final Method STRING_GETTER = method("getStringField");
  private static final Method INT_GETTER = method("getIntField");
  private static final Method NOT_GETTER = method("notGetter");

  private static Method method(String methodName) {
    try {
      return Pojo.class.getDeclaredMethod(methodName);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Unable to find method '" + methodName + "'");
    }
  }

  @Test
  public void testInitializedWithCorrectNames() {
    ReflectionGetter stringGetter = new ReflectionGetter(STRING_GETTER);
    ReflectionGetter intGetter = new ReflectionGetter(INT_GETTER);
    ReflectionGetter notGetter = new ReflectionGetter(NOT_GETTER);

    assertEquals("stringField", stringGetter.name());
    assertEquals("intField", intGetter.name());
    assertEquals("notGetter", notGetter.name());
  }


  @Test
  public void testInitializedWithCorrectTypes() {
    ReflectionGetter stringGetter = new ReflectionGetter(STRING_GETTER);
    ReflectionGetter intGetter = new ReflectionGetter(INT_GETTER);
    ReflectionGetter notGetter = new ReflectionGetter(NOT_GETTER);

    assertEquals(String.class, stringGetter.type());
    assertEquals(Integer.class, intGetter.type());
    assertEquals(Integer.class, notGetter.type());
  }

  @Test
  public void testInvokesCorrectGetter() {
    Pojo pojo = new Pojo();

    ReflectionGetter stringGetter = new ReflectionGetter(STRING_GETTER);
    ReflectionGetter intGetter = new ReflectionGetter(INT_GETTER);
    ReflectionGetter notGetter = new ReflectionGetter(NOT_GETTER);

    assertEquals("test", stringGetter.get(pojo));
    assertEquals(3421, intGetter.get(pojo));
    assertEquals(542, notGetter.get(pojo));
  }
}
