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
import static org.junit.Assert.fail;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test aims at keeping the public API is conformant to a hard-coded policy by
 * testing whether a package (determined by the location of the test) exposes only white listed
 * packages/classes.
 * <p>
 * Tests that derive from {@link ApiSurfaceVerification} should be placed under the package to be
 * tested and implement {@link ApiSurfaceVerification#allowedPackages()}. Further customization
 * can be done by overriding {@link ApiSurfaceVerification#apiSurface()}.
 * </p>
 */
// based on previous code by @kennknowles and others.
@RunWith(JUnit4.class)
public abstract class ApiSurfaceVerification {

  private static final Logger LOG = LoggerFactory.getLogger(ApiSurfaceVerification.class);

  protected abstract Set<Matcher<? extends Class<?>>>  allowedPackages();

  private ApiSurface prune(final ApiSurface apiSurface, final Set<String> prunePatterns) {
    ApiSurface prunedApiSurface = apiSurface;
    for (final String prunePattern : prunePatterns) {
      prunedApiSurface = prunedApiSurface.pruningPattern(prunePattern);
    }
    return prunedApiSurface;
  }

  protected Set<String> prunePatterns() {
    return Sets.newHashSet("org[.]apache[.]beam[.].*Test.*",
                           "org[.]apache[.]beam[.].*IT",
                           "java[.]lang.*");
  }

  protected ApiSurface apiSurface() throws IOException {
    final String thisPackage = getClass().getPackage().getName();
    LOG.info("Verifying the surface of API package: {}", thisPackage);
    final ApiSurface apiSurface = ApiSurface.ofPackage(thisPackage);
    return prune(apiSurface, prunePatterns());
  }

  private void assertApiSurface(final ApiSurface checkedApiSurface,
                                final Set<Matcher<? extends Class<?>>> allowedPackages)
      throws Exception {

    final Map<Class<?>, List<Class<?>>> disallowedClasses = Maps.newHashMap();
    for (final Class<?> clazz : checkedApiSurface.getExposedClasses()) {
      if (!classIsAllowed(clazz, allowedPackages)) {
        disallowedClasses.put(clazz, checkedApiSurface.getAnyExposurePath(clazz));
      }
    }

    final List<String> disallowedMessages = Lists.newArrayList();
    for (final Map.Entry<Class<?>, List<Class<?>>> entry : disallowedClasses.entrySet()) {
      disallowedMessages.add(entry.getKey() + " exposed via:\n\t\t"
      + Joiner.on("\n\t\t").join(entry.getValue()));
    }
    Collections.sort(disallowedMessages);

    if (!disallowedMessages.isEmpty()) {
      fail("The following disallowed classes appear in the public API surface of the SDK:\n\t"
        + Joiner.on("\n\t").join(disallowedMessages));
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private boolean classIsAllowed(final Class<?> clazz,
                                 final Set<Matcher<? extends Class<?>>>  allowedPackages) {
    // Safe cast inexpressible in Java without rawtypes
    return anyOf((Iterable) allowedPackages).matches(clazz);
  }

  protected static Matcher<Class<?>> inPackage(final String packageName) {
    return new ClassInPackage(packageName);
  }

  private static class ClassInPackage extends TypeSafeDiagnosingMatcher<Class<?>> {

    private final String packageName;

    private ClassInPackage(final String packageName) {
      this.packageName = packageName;
    }

    @Override
    public void describeTo(final Description description) {
      description.appendText("Class in package \"");
      description.appendText(packageName);
      description.appendText("\"");
    }

    @Override
    protected boolean matchesSafely(final Class<?> clazz, final Description mismatchDescription) {
      return clazz.getName().startsWith(packageName + ".");
    }
  }

  @Test
  public void testApiSurface() throws Exception {
    assertApiSurface(apiSurface(), allowedPackages());
  }
}
