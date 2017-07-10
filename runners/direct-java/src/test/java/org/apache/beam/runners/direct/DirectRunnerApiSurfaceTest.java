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

package org.apache.beam.runners.direct;

import static org.apache.beam.sdk.util.ApiSurface.containsOnlyPackages;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.beam.sdk.util.ApiSurface;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** API surface verification for {@link org.apache.beam.runners.direct}. */
@RunWith(JUnit4.class)
public class DirectRunnerApiSurfaceTest {
  @Test
  public void testDirectRunnerApiSurface() throws Exception {
    // The DirectRunner can expose the Core SDK, anything exposed by the Core SDK, and itself
    @SuppressWarnings("unchecked")
    final Set<String> allowed =
        ImmutableSet.of("org.apache.beam.sdk", "org.apache.beam.runners.direct", "org.joda.time");
    ApiSurface apiSurface =
        new DirectRunnerApiSurface(Collections.<Class<?>>emptySet(),
                Collections.<Pattern>emptySet())
            .buildApiSurface();

    assertThat(apiSurface, containsOnlyPackages(allowed));
  }
}
