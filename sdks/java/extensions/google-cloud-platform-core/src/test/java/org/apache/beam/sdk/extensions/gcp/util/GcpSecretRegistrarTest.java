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
package org.apache.beam.sdk.extensions.gcp.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.util.SecretRegistrar;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcpSecretRegistrar} and {@link GcpHsmGeneratedSecretRegistrar}. */
@RunWith(JUnit4.class)
public class GcpSecretRegistrarTest {

  @Test
  public void testGcpSecretRegistrarServiceLoader() {
    for (SecretRegistrar registrar :
        Lists.newArrayList(ServiceLoader.load(SecretRegistrar.class).iterator())) {
      if (registrar instanceof GcpSecretRegistrar) {
        Map<String, SecretRegistrar.SecretFactory> factories = registrar.getSecretFactories();
        assertThat(factories.keySet(), hasItems("GoogleCloudSecretManager", "GcpSecret"));
        return;
      }
    }
    fail("Expected to find " + GcpSecretRegistrar.class);
  }

  @Test
  public void testGcpHsmGeneratedSecretRegistrarServiceLoader() {
    for (SecretRegistrar registrar :
        Lists.newArrayList(ServiceLoader.load(SecretRegistrar.class).iterator())) {
      if (registrar instanceof GcpHsmGeneratedSecretRegistrar) {
        Map<String, SecretRegistrar.SecretFactory> factories = registrar.getSecretFactories();
        assertThat(
            factories.keySet(),
            hasItems("GoogleCloudHsmGeneratedSecretManager", "GcpHsmGeneratedSecret"));
        return;
      }
    }
    fail("Expected to find " + GcpHsmGeneratedSecretRegistrar.class);
  }
}
