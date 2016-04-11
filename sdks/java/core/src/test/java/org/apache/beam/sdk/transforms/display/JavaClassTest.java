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

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.util.SerializableUtils;
import com.google.common.testing.EqualsTester;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link JavaClass}.
 */
@RunWith(JUnit4.class)
public class JavaClassTest {
  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void testProperties() {
    JavaClass thisClass = JavaClass.of(JavaClassTest.class);
    assertEquals(JavaClassTest.class.getName(), thisClass.getName());
    assertEquals(JavaClassTest.class.getSimpleName(), thisClass.getSimpleName());
  }

  @Test
  public void testInputValidation() {
    thrown.expect(NullPointerException.class);
    JavaClass.of(null);
  }

  @Test
  public void testEquality() {
    new EqualsTester()
        .addEqualityGroup(JavaClass.of(JavaClassTest.class), JavaClass.fromInstance(this))
        .addEqualityGroup(JavaClass.of(JavaClass.class))
        .addEqualityGroup(JavaClass.of(Class.class))
        .testEquals();
  }

  @Test
  public void testSerialization() {
    SerializableUtils.ensureSerializable(JavaClass.of(JavaClassTest.class));
  }
}
