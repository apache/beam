/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.transforms.reflect;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.beam.sdk.testing.InterceptingUrlClassLoader;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;

/**
 * Tests for the proxy generator based on byte buddy.
 */
public class ByteBuddyDoFnInvokerFactoryTest {
  /**
   * Ensuring we define the subclass using bytebuddy in the right classloader,
   * i.e. the doFn classloader and not beam classloader.
   */
  @Test
  public void validateProxyClassLoaderSelectionLogic() throws Exception {
    final ClassLoader testLoader = Thread.currentThread().getContextClassLoader();
    final ClassLoader loader = new InterceptingUrlClassLoader(testLoader, MyDoFn.class.getName());
    final Class<? extends DoFn<?, ?>> source = (Class<? extends DoFn<?, ?>>) loader.loadClass(
        "org.apache.beam.sdk.transforms.reflect.ByteBuddyDoFnInvokerFactoryTest$MyDoFn");
    assertEquals(loader, source.getClassLoader()); // precondition check
    final String proxyName = source.getName()
      + StableInvokerNamingStrategy.PROXY_NAME_DELIMITER
      + ByteBuddyDoFnInvokerFactory.PROXY_CLASSNAME_SUFFIX;
    for (final ClassLoader cl : asList(testLoader, loader)) {
      try {
        cl.loadClass(proxyName);
        fail("proxy shouldn't already exist");
      } catch (final ClassNotFoundException cnfe) {
        // exactly what we expected!
      }
    }
    final DoFnInvoker<?, ?> subclass = ByteBuddyDoFnInvokerFactory.only()
            .invokerFor(source.getConstructor().newInstance());
    try {
      testLoader.loadClass(proxyName);
      fail("proxy shouldn't already exist");
    } catch (final ClassNotFoundException cnfe) {
      // this is the regression this test prevents
    }
    assertEquals(subclass.getClass().getClassLoader(), loader);
  }

  /**
   * a sample dofn to test proxy classloader selection.
   */
  public static class MyDoFn extends DoFn<String, String> {
    @ProcessElement
    public void onElement(final ProcessContext ctx) {
      // no-op
    }
  }
}
