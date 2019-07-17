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

import static org.apache.beam.sdk.util.ApiSurface.containsOnlyClassesMatching;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertThat;

import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Functionality tests for ApiSurface. */
@RunWith(JUnit4.class)
public class ApiSurfaceTest {

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void assertExposed(final Class classToExamine, final Class... exposedClasses) {

    final ApiSurface apiSurface = ApiSurface.ofClass(classToExamine).pruningPrefix("java");

    final ImmutableSet<Matcher<Class<?>>> allowed =
        FluentIterable.from(
                Iterables.concat(Sets.newHashSet(classToExamine), Sets.newHashSet(exposedClasses)))
            .transform(Matchers::<Class<?>>equalTo)
            .toSet();

    assertThat(apiSurface, containsOnlyClassesMatching(allowed));
  }

  private interface Exposed {}

  private interface ExposedReturnType {
    Exposed zero();
  }

  @Test
  public void testExposedReturnType() throws Exception {
    assertExposed(ExposedReturnType.class, Exposed.class);
  }

  private interface ExposedParameterTypeVarBound {
    <T extends Exposed> void getList(T whatever);
  }

  @Test
  public void testExposedParameterTypeVarBound() throws Exception {
    assertExposed(ExposedParameterTypeVarBound.class, Exposed.class);
  }

  private interface ExposedWildcardBound {
    void acceptList(List<? extends Exposed> arg);
  }

  @Test
  public void testExposedWildcardBound() throws Exception {
    assertExposed(ExposedWildcardBound.class, Exposed.class);
  }

  private interface ExposedActualTypeArgument extends List<Exposed> {}

  @Test
  public void testExposedActualTypeArgument() throws Exception {
    assertExposed(ExposedActualTypeArgument.class, Exposed.class);
  }

  @Test
  public void testIgnoreAll() throws Exception {
    ApiSurface apiSurface =
        ApiSurface.ofClass(ExposedWildcardBound.class)
            .includingClass(Object.class)
            .includingClass(ApiSurface.class)
            .pruningPattern(".*");
    assertThat(apiSurface.getExposedClasses(), emptyIterable());
  }

  private interface PrunedPattern {}

  private interface NotPruned extends PrunedPattern {}

  @Test
  public void testprunedPattern() throws Exception {
    ApiSurface apiSurface = ApiSurface.ofClass(NotPruned.class).pruningClass(PrunedPattern.class);
    assertThat(apiSurface.getExposedClasses(), containsInAnyOrder((Class) NotPruned.class));
  }

  private interface ExposedTwice {
    Exposed zero();

    Exposed one();
  }

  @Test
  public void testExposedTwice() throws Exception {
    assertExposed(ExposedTwice.class, Exposed.class);
  }

  private interface ExposedCycle {
    ExposedCycle zero(Exposed foo);
  }

  @Test
  public void testExposedCycle() throws Exception {
    assertExposed(ExposedCycle.class, Exposed.class);
  }

  private interface ExposedGenericCycle {
    Exposed zero(List<ExposedGenericCycle> foo);
  }

  @Test
  public void testExposedGenericCycle() throws Exception {
    assertExposed(ExposedGenericCycle.class, Exposed.class);
  }

  private interface ExposedArrayCycle {
    Exposed zero(ExposedArrayCycle[] foo);
  }

  @Test
  public void testExposedArrayCycle() throws Exception {
    assertExposed(ExposedArrayCycle.class, Exposed.class);
  }
}
