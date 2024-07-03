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

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AutoValueUtilsTest {

  @AutoValue
  public abstract static class SimpleAutoValue {
    public abstract String getStr();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setStr(String value);

      public abstract SimpleAutoValue build();
    }
  }

  @AutoValue
  public abstract static class GenericAutoValue<T, NumberT extends Number> {
    public abstract T getT();

    public abstract NumberT getN();

    @AutoValue.Builder
    public abstract static class Builder<T, NumberT extends Number> {
      public abstract Builder<T, NumberT> setT(T value);

      public abstract Builder<T, NumberT> setN(NumberT value);

      public abstract GenericAutoValue<T, NumberT> build();
    }
  }

  @Test
  public void testGetBaseAutoValueClass() throws Exception {
    TypeDescriptor<?> actual =
        AutoValueUtils.getBaseAutoValueClass(
            TypeDescriptor.of(AutoValue_AutoValueUtilsTest_SimpleAutoValue.class));

    assertEquals(TypeDescriptor.of(SimpleAutoValue.class), actual);
  }

  @Test
  public void testGetBaseAutoValueClassGeneric() throws Exception {
    TypeDescriptor<?> actual =
        AutoValueUtils.getBaseAutoValueClass(
            new TypeDescriptor<
                AutoValue_AutoValueUtilsTest_GenericAutoValue<String, Integer>>() {});

    assertEquals(new TypeDescriptor<GenericAutoValue<String, Integer>>() {}, actual);
  }

  @Test
  public void testGetAutoValueGenerated() throws Exception {
    TypeDescriptor<?> actual =
        AutoValueUtils.getAutoValueGenerated(TypeDescriptor.of(SimpleAutoValue.class));
    assertEquals(TypeDescriptor.of(AutoValue_AutoValueUtilsTest_SimpleAutoValue.class), actual);
  }

  @Test
  public void testGetAutoValueGeneratedGeneric() throws Exception {
    TypeDescriptor<?> actual =
        AutoValueUtils.getAutoValueGenerated(
            new TypeDescriptor<GenericAutoValue<String, Integer>>() {});
    assertEquals(
        new TypeDescriptor<AutoValue_AutoValueUtilsTest_GenericAutoValue<String, Integer>>() {},
        actual);
  }

  @Test
  public void testGetAutoValueGeneratedBuilder() throws Exception {
    TypeDescriptor<?> actual =
        AutoValueUtils.getAutoValueGeneratedBuilder(TypeDescriptor.of(SimpleAutoValue.class));
    assertEquals(
        TypeDescriptor.of(AutoValue_AutoValueUtilsTest_SimpleAutoValue.Builder.class), actual);
  }

  @Test
  public void testGetAutoValueGeneratedBuilderGeneric() throws Exception {
    TypeDescriptor<?> actual =
        AutoValueUtils.getAutoValueGeneratedBuilder(
            new TypeDescriptor<GenericAutoValue<Map<String, String>, Integer>>() {});
    assertEquals(
        new TypeDescriptor<
            AutoValue_AutoValueUtilsTest_GenericAutoValue.Builder<
                Map<String, String>, Integer>>() {},
        actual);
  }
}
