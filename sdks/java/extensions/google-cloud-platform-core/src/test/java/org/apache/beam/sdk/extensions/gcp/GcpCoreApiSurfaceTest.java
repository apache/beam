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
package org.apache.beam;

import static org.apache.beam.sdk.util.ApiSurface.containsOnlyPackages;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.beam.sdk.util.ApiSurface;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** API surface verification for Google Cloud Platform core components. */
@RunWith(JUnit4.class)
public class GcpCoreApiSurfaceTest {

  @Test
  public void testApiSurface() throws Exception {

    @SuppressWarnings("unchecked")
    final Set<String> allowed =
        ImmutableSet.of(
            "org.apache.beam",
            "com.google.api.client",
            "com.google.api.services.storage",
            "com.google.auth",
            "com.fasterxml.jackson.annotation",
            "com.fasterxml.jackson.core",
            "com.fasterxml.jackson.databind",
            "org.apache.avro",
            "org.hamcrest",
            // via DataflowMatchers
            "org.codehaus.jackson",
            // via Avro
            "org.joda.time",
            "org.junit");

    assertThat(
        ApiSurface.getSdkApiSurface(getClass().getClassLoader()), containsOnlyPackages(allowed));
  }
}
