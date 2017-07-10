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
package org.apache.beam.sdk.extensions.gcp;

import static org.apache.beam.sdk.util.ApiSurface.classesInPackage;
import static org.apache.beam.sdk.util.ApiSurface.containsOnlyClassesMatching;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.beam.sdk.util.ApiSurface;
import org.apache.beam.sdk.util.GcpCoreApiSurface;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** API surface verification for Google Cloud Platform core components. */
@RunWith(JUnit4.class)
public class GcpCoreApiSurfaceTest {

  @Test
  public void testGcpCoreApiSurface() throws Exception {
    final ApiSurface apiSurface = new GcpCoreApiSurface(Collections.<Class<?>>emptySet(),
            Collections.<Pattern>emptySet())
            .buildApiSurface();

    @SuppressWarnings("unchecked")
    final Set<Matcher<Class<?>>> allowedClasses =
        ImmutableSet.of(
            classesInPackage("com.google.api.client.googleapis"),
            classesInPackage("com.google.api.client.http"),
            classesInPackage("com.google.api.client.json"),
            classesInPackage("com.google.api.client.util"),
            classesInPackage("com.google.api.services.storage"),
            classesInPackage("com.google.auth"),
            classesInPackage("com.fasterxml.jackson.annotation"),
            classesInPackage("java"),
            classesInPackage("javax"),
            classesInPackage("org.apache.beam.sdk"),
            classesInPackage("org.joda.time")
        );

    assertThat(apiSurface, containsOnlyClassesMatching(allowedClasses));
  }
}
