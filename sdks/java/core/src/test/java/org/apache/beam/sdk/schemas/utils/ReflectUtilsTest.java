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
package org.apache.beam.sdk.schemas.utils;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Method;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ReflectUtils}. */
@RunWith(JUnit4.class)
public class ReflectUtilsTest {

  @Test
  public void testGetMethodsOrderIndependent() {
    List<Method> methods1 = ReflectUtils.getMethods(Record1.class);
    List<Method> methods2 = ReflectUtils.getMethods(Record2.class);
    assertEquals(methods1.size(), methods2.size());
    for (int i = 0; i < methods2.size(); i++) {
      assertEquals(methods1.get(i).getName(), methods2.get(i).getName());
    }
  }

  public abstract static class Record1 {
    public abstract @Nullable String getDestination();

    public abstract @Nullable Long getExpiration();

    public abstract String getMessageId();
  }

  public abstract static class Record2 {
    public abstract @Nullable Long getExpiration();

    public abstract String getMessageId();

    public abstract @Nullable String getDestination();
  }
}
