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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.ProviderNotFoundException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.beam.sdk.extensions.sql.impl.parser.SqlCreateFunctionTest;
import org.apache.beam.sdk.extensions.sql.udf.UdfProvider;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JavaUdfLoader}. */
@RunWith(JUnit4.class)
public class JavaUdfLoaderTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private final String jarPathProperty = "beam.sql.udf.test.jar_path";
  private final String emptyJarPathProperty = "beam.sql.udf.test.empty_jar_path";

  private final String jarPath = System.getProperty(jarPathProperty, "");
  private final String emptyJarPath = System.getProperty(emptyJarPathProperty, "");

  @Before
  public void setUp() {
    if (jarPath == null) {
      fail(
          String.format(
              "System property %s must be set to run %s.",
              jarPathProperty, SqlCreateFunctionTest.class.getSimpleName()));
    }
    if (emptyJarPath == null) {
      fail(
          String.format(
              "System property %s must be set to run %s.",
              emptyJarPathProperty, SqlCreateFunctionTest.class.getSimpleName()));
    }
  }

  /**
   * Test that the parent classloader does not load any implementations of {@link UdfProvider}. This
   * is important because we do not want to pollute the user's namespace.
   */
  @Test
  public void testClassLoaderHasNoUdfProviders() throws IOException {
    JavaUdfLoader udfLoader = new JavaUdfLoader();
    Iterator<UdfProvider> udfProviders =
        udfLoader.getUdfProviders(ReflectHelpers.findClassLoader());
    assertFalse(udfProviders.hasNext());
  }

  @Test
  public void testLoadScalarFunction() {
    JavaUdfLoader udfLoader = new JavaUdfLoader();
    udfLoader.loadScalarFunction(Collections.singletonList("helloWorld"), jarPath);
  }

  @Test
  public void testLoadAggregateFunction() {
    JavaUdfLoader udfLoader = new JavaUdfLoader();
    udfLoader.loadAggregateFunction(Collections.singletonList("my_sum"), jarPath);
  }

  @Test
  public void testLoadUnregisteredScalarFunctionThrowsRuntimeException() {
    JavaUdfLoader udfLoader = new JavaUdfLoader();
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(
        String.format("No implementation of scalar function notRegistered found in %s.", jarPath));
    udfLoader.loadScalarFunction(Collections.singletonList("notRegistered"), jarPath);
  }

  @Test
  public void testLoadUnregisteredAggregateFunctionThrowsRuntimeException() {
    JavaUdfLoader udfLoader = new JavaUdfLoader();
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(
        String.format(
            "No implementation of aggregate function notRegistered found in %s.", jarPath));
    udfLoader.loadAggregateFunction(Collections.singletonList("notRegistered"), jarPath);
  }

  @Test
  public void testJarMissingUdfProviderThrowsProviderNotFoundException() {
    JavaUdfLoader udfLoader = new JavaUdfLoader();
    thrown.expect(ProviderNotFoundException.class);
    thrown.expectMessage(String.format("No UdfProvider implementation found in %s.", emptyJarPath));
    // Load from an inhabited jar first so we can make sure we load UdfProviders in isolation
    // from other jars.
    udfLoader.loadScalarFunction(Collections.singletonList("helloWorld"), jarPath);
    udfLoader.loadScalarFunction(Collections.singletonList("helloWorld"), emptyJarPath);
  }
}
