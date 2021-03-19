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
package org.apache.beam.sdk.coders;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

/**
 * A command-line utility for printing the base-64 encodings of test values, for generating exact
 * wire format tests.
 *
 * <p>For internal use only.
 *
 * <p>Example invocation via maven: {@code mvn test-compile exec:java \ -Dexec.classpathScope=test \
 * -Dexec.mainClass=org.apache.beam.sdk.coders.PrintBase64Encodings
 * -Dexec.args='org.apache.beam.sdk.coders.BigEndianIntegerCoderTest.TEST_CODER \
 * org.apache.beam.sdk.coders.BigEndianIntegerCoderTest.TEST_VALUES' }
 */
public class PrintBase64Encodings {

  /** Gets a field even if it is private, which the test data generally will be. */
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
