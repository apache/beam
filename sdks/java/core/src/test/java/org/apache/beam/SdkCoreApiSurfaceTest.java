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

import static org.apache.beam.sdk.util.ApiSurface.Matchers.classesInPackage;
import static org.apache.beam.sdk.util.ApiSurface.Matchers.containsOnlyClassesMatching;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.beam.sdk.util.ApiSurface;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * API surface verification for {@link org.apache.beam}.
 */
@RunWith(JUnit4.class)
public class SdkCoreApiSurfaceTest {

  @SuppressWarnings("unchecked")
  private final Set<Matcher<Class<?>>> allowedClasses =
      ImmutableSet.of(classesInPackage("org.apache.beam"),
                      classesInPackage("com.google.api.client"),
                      classesInPackage("com.google.api.services.bigquery"),
                      classesInPackage("com.google.api.services.cloudresourcemanager"),
                      classesInPackage("com.google.api.services.pubsub"),
                      classesInPackage("com.google.api.services.storage"),
                      classesInPackage("com.google.auth"),
                      classesInPackage("com.google.protobuf"),
                      classesInPackage("com.fasterxml.jackson.annotation"),
                      classesInPackage("com.fasterxml.jackson.core"),
                      classesInPackage("com.fasterxml.jackson.databind"),
                      classesInPackage("org.apache.avro"),
                      classesInPackage("org.hamcrest"),
                      // via DataflowMatchers
                      classesInPackage("org.codehaus.jackson"),
                      // via Avro
                      classesInPackage("org.joda.time"),
                      classesInPackage("org.junit"));

  @Test
  public void testSdkApiSurface() throws Exception {
    assertThat(ApiSurface.getSdkApiSurface(), containsOnlyClassesMatching(allowedClasses));
  }
}
