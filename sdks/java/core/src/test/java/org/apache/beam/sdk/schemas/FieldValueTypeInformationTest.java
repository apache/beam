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
package org.apache.beam.sdk.schemas;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;

public class FieldValueTypeInformationTest {
  public static class GenericClass<T> {
    public T t;

    public GenericClass(T t) {
      this.t = t;
    }

    public T getT() {
      return t;
    }

    public void setT(T t) {
      this.t = t;
    }
  }

  private final TypeDescriptor<GenericClass<Map<String, Integer>>> typeDescriptor =
      new TypeDescriptor<GenericClass<Map<String, Integer>>>() {};
  private final TypeDescriptor<Map<String, Integer>> expectedFieldTypeDescriptor =
      new TypeDescriptor<Map<String, Integer>>() {};

  @Test
  public void testForGetter() throws Exception {
    FieldValueTypeInformation actual =
        FieldValueTypeInformation.forGetter(
            typeDescriptor, GenericClass.class.getMethod("getT"), 0);
    assertEquals(expectedFieldTypeDescriptor, actual.getType());
  }

  @Test
  public void testForField() throws Exception {
    FieldValueTypeInformation actual =
        FieldValueTypeInformation.forField(typeDescriptor, GenericClass.class.getField("t"), 0);
    assertEquals(expectedFieldTypeDescriptor, actual.getType());
  }

  @Test
  public void testForSetter() throws Exception {
    FieldValueTypeInformation actual =
        FieldValueTypeInformation.forSetter(
            typeDescriptor, GenericClass.class.getMethod("setT", Object.class));
    assertEquals(expectedFieldTypeDescriptor, actual.getType());
  }
}
