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

package com.google.cloud.dataflow.sdk.coders;

import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

/**
 * A command-line utility for printing the base-64 encodings of test values, for generating exact
 * wire format tests.
 *
 * <p>For internal use only.
 *
 * <p>Example invocation via maven:
 * {@code
 *   mvn test-compile exec:java \
 *       -Dexec.classpathScope=test \
 *       -Dexec.mainClass=com.google.cloud.dataflow.sdk.coders.PrintBase64Encodings
 *       -Dexec.args='com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoderTest.TEST_CODER \
 *           com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoderTest.TEST_VALUES'
 * }
 */
public class PrintBase64Encodings {

  /**
   * Gets a field even if it is private, which the test data generally will be.
   */
  private static Field getField(Class<?> clazz, String fieldName) throws Exception {
    for (Field field : clazz.getDeclaredFields()) {
      if (field.getName().equals(fieldName)) {
        if (!Modifier.isPublic(field.getModifiers())) {
          field.setAccessible(true);
        }
        return field;
      }
    }
    throw new NoSuchFieldException(clazz.getCanonicalName() + "." + fieldName);
  }

  private static Object getFullyQualifiedValue(String fullyQualifiedName) throws Exception {
    int lastDot = fullyQualifiedName.lastIndexOf(".");
    String className = fullyQualifiedName.substring(0, lastDot);
    String fieldName = fullyQualifiedName.substring(lastDot + 1);

    Class<?> clazz = Class.forName(className);
    Field field = getField(clazz, fieldName);
    return field.get(null);
  }

  public static void main(String[] argv) throws Exception {
    @SuppressWarnings("unchecked")
    Coder<Object> testCoder = (Coder<Object>) getFullyQualifiedValue(argv[0]);
    @SuppressWarnings("unchecked")
    List<Object> testValues = (List<Object>) getFullyQualifiedValue(argv[1]);

    List<String> base64Encodings = Lists.newArrayList();
    for (Object value : testValues) {
      base64Encodings.add(CoderUtils.encodeToBase64(testCoder, value));
    }
    System.out.println(String.format("\"%s\"", Joiner.on("\",\n\"").join(base64Encodings)));
  }
}
