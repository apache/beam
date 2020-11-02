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
package org.apache.beam.sdk.extensions.sql.zetasql.translation;

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Iterator;
import org.apache.beam.sdk.extensions.sql.UdfProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JavaUdfLoader}. */
@RunWith(JUnit4.class)
public class JavaUdfLoaderTest {
  /**
   * Test that the parent classloader in {@link JavaUdfLoader} does not load any implementations of
   * {@link UdfProvider}. This is important because we do not want to pollute the user's namespace.
   */
  @Test
  public void testClassLoaderHasNoUdfProviders() throws IOException {
    JavaUdfLoader udfLoader = new JavaUdfLoader();
    Iterator<UdfProvider> udfProviders =
        udfLoader.getUdfProviders(Thread.currentThread().getContextClassLoader());
    assertFalse(udfProviders.hasNext());
  }
}
