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
package org.apache.beam.sdk.transforms.display;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Java 8 tests for {@link ClassForDisplay}.
 */
@RunWith(JUnit4.class)
public class ClassForDisplayJava8Test implements Serializable {
  @Test
  public void testLambdaClassSerialization() {
    final SerializableFunction<Object, Object> f = x -> x;
    Serializable myClass = new Serializable() {
      // Class references for lambdas do not serialize, which is why we support ClassForDisplay
      // Specifically, the following would not work:
      // Class<?> clazz = f.getClass();
      ClassForDisplay javaClass = ClassForDisplay.fromInstance(f);
    };

    SerializableUtils.ensureSerializable(myClass);
  }
}
