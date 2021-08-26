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
package org.apache.beam.runners.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link InMemoryBundleFinalizer}. */
@RunWith(JUnit4.class)
public class InMemoryBundleFinalizerTest {
  @Test
  public void testCallbackRegistration() {
    InMemoryBundleFinalizer finalizer = new InMemoryBundleFinalizer();
    // Check when nothing has been registered
    assertThat(finalizer.getAndClearFinalizations(), is(empty()));
    finalizer.afterBundleCommit(new Instant(), () -> {});
    assertThat(finalizer.getAndClearFinalizations(), hasSize(1));
    // Check that it is empty
    assertThat(finalizer.getAndClearFinalizations(), is(empty()));
  }
}
