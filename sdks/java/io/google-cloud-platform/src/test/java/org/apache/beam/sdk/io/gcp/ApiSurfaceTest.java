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
package org.apache.beam.sdk.io.gcp;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.util.ApiSurface;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the API surface of the gcp-io module. Tests that our public API is conformant to a
 * hard-coded policy.
 */
@RunWith(JUnit4.class)
public class ApiSurfaceTest {

  @Test
  public void testOurApiSurface() throws Exception {
    ApiSurface checkedApiSurface =
        ApiSurface.ofPackage("org.apache.beam.sdk.io.gcp")
            .pruningPattern("org[.]apache[.]beam[.].*Test.*")
            .pruningPattern("org[.]apache[.]beam[.].*IT")
            .pruningPattern("java[.]lang.*");

    Map<Class<?>, List<Class<?>>> disallowedClasses = Maps.newHashMap();
    for (Class<?> clazz : checkedApiSurface.getExposedClasses()) {
      if (!classIsAllowed(clazz)) {
        disallowedClasses.put(clazz, checkedApiSurface.getAnyExposurePath(clazz));
      }
    }

    List<String> disallowedMessages = Lists.newArrayList();
    for (Map.Entry<Class<?>, List<Class<?>>> entry : disallowedClasses.entrySet()) {
      disallowedMessages.add(entry.getKey() + " exposed via:\n\t\t"
      + Joiner.on("\n\t\t").join(entry.getValue()));
    }
    Collections.sort(disallowedMessages);

    if (!disallowedMessages.isEmpty()) {
      fail("The following disallowed classes appear in the public API surface of the SDK:\n\t"
        + Joiner.on("\n\t").join(disallowedMessages));
    }
  }

  @SuppressWarnings("unchecked")
  private static final Set<Matcher<? extends Class<?>>> ALLOWED_PACKAGES =
      ImmutableSet.<Matcher<? extends Class<?>>>of(
          inPackage("com.google.api.client.json"),
          inPackage("com.google.api.client.util"),
          inPackage("com.google.api.services.bigquery.model"),
          inPackage("com.google.auth"),
          inPackage("com.google.bigtable.v2"),
          inPackage("com.google.cloud.bigtable.config"),
          equalTo(BigtableInstanceName.class),
          equalTo(BigtableTableName.class),
          // https://github.com/GoogleCloudPlatform/cloud-bigtable-client/pull/1056
          inPackage("com.google.common.collect"), // via Bigtable, PR above out to fix.
          inPackage("com.google.datastore.v1"),
          inPackage("com.google.protobuf"),
          inPackage("com.google.type"),
          inPackage("com.fasterxml.jackson.annotation"),
          inPackage("com.fasterxml.jackson.core"),
          inPackage("com.fasterxml.jackson.databind"),
          inPackage("io.grpc"),
          inPackage("java"),
          inPackage("javax"),
          inPackage("org.apache.beam"),
          inPackage("org.apache.commons.logging"), // via Bigtable
          inPackage("org.joda.time"));

  @SuppressWarnings({"rawtypes", "unchecked"})
  private boolean classIsAllowed(Class<?> clazz) {
    // Safe cast inexpressible in Java without rawtypes
    return anyOf((Iterable) ALLOWED_PACKAGES).matches(clazz);
  }

  private static Matcher<Class<?>> inPackage(String packageName) {
    return new ClassInPackage(packageName);
  }

  private static class ClassInPackage extends TypeSafeDiagnosingMatcher<Class<?>> {

    private final String packageName;

    public ClassInPackage(String packageName) {
      this.packageName = packageName;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Class in package \"");
      description.appendText(packageName);
      description.appendText("\"");
    }

    @Override
    protected boolean matchesSafely(Class<?> clazz, Description mismatchDescription) {
      return clazz.getName().startsWith(packageName + ".");
    }

  }
}
