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
package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for ApiSurface. These both test the functionality and also that our
 * public API is conformant to a hard-coded policy.
 */
@RunWith(JUnit4.class)
public class ApiSurfaceTest {

  @Test
  public void testOurApiSurface() throws Exception {
    ApiSurface checkedApiSurface = ApiSurface.getSdkApiSurface();

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
  private static final Set<Matcher<Class<?>>> ALLOWED_PACKAGES =
      ImmutableSet.of(
          inPackage("org.apache.beam"),
          inPackage("com.google.api.client"),
          inPackage("com.google.api.services.bigquery"),
          inPackage("com.google.api.services.cloudresourcemanager"),
          inPackage("com.google.api.services.dataflow"),
          inPackage("com.google.api.services.pubsub"),
          inPackage("com.google.api.services.storage"),
          inPackage("com.google.auth"),
          inPackage("com.google.bigtable.v1"),
          inPackage("com.google.cloud.bigtable.config"),
          inPackage("com.google.cloud.bigtable.grpc"),
          inPackage("com.google.datastore"),
          inPackage("com.google.protobuf"),
          inPackage("com.google.rpc"),
          inPackage("com.google.type"),
          inPackage("com.fasterxml.jackson.annotation"),
          inPackage("io.grpc"),
          inPackage("org.apache.avro"),
          inPackage("org.apache.commons.logging"), // via BigTable
          inPackage("org.hamcrest"), // via DataflowMatchers
          inPackage("org.codehaus.jackson"), // via Avro
          inPackage("org.joda.time"),
          inPackage("org.junit"),
          inPackage("java"));

  @SuppressWarnings({"rawtypes", "unchecked"})
  private boolean classIsAllowed(Class<?> clazz) {
    // Safe cast inexpressible in Java without rawtypes
    return anyOf((Iterable) ALLOWED_PACKAGES).matches(clazz);
  }

  private static final Matcher<Class<?>> inPackage(String packageName) {
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

  //////////////////////////////////////////////////////////////////////////////////

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void assertExposed(Class classToExamine, Class... exposedClasses) {
    ApiSurface apiSurface = ApiSurface
        .ofClass(classToExamine)
        .pruningPrefix("java");

    Set<Class> expectedExposed = Sets.newHashSet(classToExamine);
    for (Class clazz : exposedClasses) {
      expectedExposed.add(clazz);
    }
    assertThat(apiSurface.getExposedClasses(), containsInAnyOrder(expectedExposed.toArray()));
  }

  private static interface Exposed { }

  private static interface ExposedReturnType {
    Exposed zero();
  }

  @Test
  public void testExposedReturnType() throws Exception {
    assertExposed(ExposedReturnType.class, Exposed.class);
  }

  private static interface ExposedParameterTypeVarBound {
    <T extends Exposed> void getList(T whatever);
  }

  @Test
  public void testExposedParameterTypeVarBound() throws Exception {
    assertExposed(ExposedParameterTypeVarBound.class, Exposed.class);
  }

  private static interface ExposedWildcardBound {
    void acceptList(List<? extends Exposed> arg);
  }

  @Test
  public void testExposedWildcardBound() throws Exception {
    assertExposed(ExposedWildcardBound.class, Exposed.class);
  }

  private static interface ExposedActualTypeArgument extends List<Exposed> { }

  @Test
  public void testExposedActualTypeArgument() throws Exception {
    assertExposed(ExposedActualTypeArgument.class, Exposed.class);
  }

  @Test
  public void testIgnoreAll() throws Exception {
    ApiSurface apiSurface = ApiSurface.ofClass(ExposedWildcardBound.class)
        .includingClass(Object.class)
        .includingClass(ApiSurface.class)
        .pruningPattern(".*");
    assertThat(apiSurface.getExposedClasses(), emptyIterable());
  }

  private static interface PrunedPattern { }
  private static interface NotPruned extends PrunedPattern { }

  @Test
  public void testprunedPattern() throws Exception {
    ApiSurface apiSurface = ApiSurface.ofClass(NotPruned.class)
        .pruningClass(PrunedPattern.class);
    assertThat(apiSurface.getExposedClasses(), containsInAnyOrder((Class) NotPruned.class));
  }

  private static interface ExposedTwice {
    Exposed zero();
    Exposed one();
  }

  @Test
  public void testExposedTwice() throws Exception {
    assertExposed(ExposedTwice.class, Exposed.class);
  }

  private static interface ExposedCycle {
    ExposedCycle zero(Exposed foo);
  }

  @Test
  public void testExposedCycle() throws Exception {
    assertExposed(ExposedCycle.class, Exposed.class);
  }

  private static interface ExposedGenericCycle {
    Exposed zero(List<ExposedGenericCycle> foo);
  }

  @Test
  public void testExposedGenericCycle() throws Exception {
    assertExposed(ExposedGenericCycle.class, Exposed.class);
  }

  private static interface ExposedArrayCycle {
    Exposed zero(ExposedArrayCycle[] foo);
  }

  @Test
  public void testExposedArrayCycle() throws Exception {
    assertExposed(ExposedArrayCycle.class, Exposed.class);
  }
}
