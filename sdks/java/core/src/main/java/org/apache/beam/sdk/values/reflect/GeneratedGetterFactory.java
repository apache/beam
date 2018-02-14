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

import static net.bytebuddy.implementation.MethodCall.invoke;
import static org.apache.beam.sdk.values.reflect.ByteBuddyUtils.implementNameGetter;
import static org.apache.beam.sdk.values.reflect.ByteBuddyUtils.implementTypeGetter;
import static org.apache.beam.sdk.values.reflect.ByteBuddyUtils.implementValueGetter;
import static org.apache.beam.sdk.values.reflect.ByteBuddyUtils.makeNewGetterInstance;
import static org.apache.beam.sdk.values.reflect.ByteBuddyUtils.subclassGetterInterface;
import static org.apache.beam.sdk.values.reflect.ReflectionUtils.getPublicGetters;
import static org.apache.beam.sdk.values.reflect.ReflectionUtils.tryStripGetPrefix;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.util.List;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;

/**
 * Implements and creates an instance of the {@link FieldValueGetter} for each public
 * getter method of the pojo class.
 *
 * <p>Generated {@link FieldValueGetter#get(Object)} calls the corresponding
 * getter method of the pojo.
 *
 * <p>Generated {@link FieldValueGetter#name()} strips the 'get' from the getter method name.
 *
 * <p>For example if pojo looks like
 * <pre>{@code
 * public class PojoClass {
 *   public String getPojoNameField() { ... }
 * }
 * }</pre>
 *
 * <p>Then, class name aside, generated {@link FieldValueGetter} will look like:
 * <pre>{@code
 * public class FieldValueGetterGenerated implements FieldValueGetter<PojoType> {
 *   public String name() {
 *     return "pojoNameField";
 *   }
 *
 *   public Class type() {
 *     return String.class;
 *   }
 *
 *   public get(PojoType pojo) {
 *     return pojo.getPojoNameField();
 *   }
 * }
 * }</pre>
 *
 * <p>ByteBuddy is used to generate the code. Class naming is left to ByteBuddy's defaults.
 *
 * <p>Class is injected into ByteBuddyUtils.class.getClassLoader().
 * See {@link ByteBuddyUtils#makeNewGetterInstance(String, DynamicType.Builder)}
 * and ByteBuddy documentation for details.
 */
class GeneratedGetterFactory implements GetterFactory {

  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  /**
   * Returns the list of the getters, one for each public getter of the pojoClass.
   */
  @Override
  public List<FieldValueGetter> generateGetters(Class pojoClass) {
    ImmutableList.Builder<FieldValueGetter> getters = ImmutableList.builder();

    List<Method> getterMethods = getPublicGetters(pojoClass);

    for (Method getterMethod : getterMethods) {
      getters.add(createFieldGetterInstance(pojoClass, getterMethod));
    }

    return getters.build();
  }

  private static FieldValueGetter createFieldGetterInstance(Class clazz, Method getterMethod) {

    DynamicType.Builder<FieldValueGetter> getterBuilder =
        subclassGetterInterface(BYTE_BUDDY, clazz);

    getterBuilder = implementNameGetter(getterBuilder, tryStripGetPrefix(getterMethod));
    getterBuilder = implementTypeGetter(getterBuilder, getterMethod.getReturnType());
    getterBuilder = implementValueGetter(getterBuilder, invoke(getterMethod).onArgument(0));

    return makeNewGetterInstance(getterMethod.getName(), getterBuilder);
  }
}
