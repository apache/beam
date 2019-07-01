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
package org.apache.beam.runners.core.metrics;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DirtyStateTest}. */
@RunWith(JUnit4.class)
public class DirtyStateTest {

  private final DirtyState dirty = new DirtyState();

  @Test
  public void basicPath() {
    assertThat("Should start dirty", dirty.beforeCommit(), is(true));
    dirty.afterCommit();
    assertThat("Should be clean after commit", dirty.beforeCommit(), is(false));

    dirty.afterModification();
    assertThat("Should be dirty after change", dirty.beforeCommit(), is(true));
    dirty.afterCommit();
    assertThat("Should be clean after commit", dirty.beforeCommit(), is(false));
  }

  @Test
  public void changeAfterBeforeCommit() {
    assertThat("Should start dirty", dirty.beforeCommit(), is(true));
    dirty.afterModification();
    dirty.afterCommit();
    assertThat(
        "Changes after beforeCommit should be dirty after afterCommit",
        dirty.beforeCommit(),
        is(true));
  }

  @Test
  public void testEquals() {
    DirtyState dirtyState = new DirtyState();
    DirtyState equal = new DirtyState();
    Assert.assertEquals(dirtyState, equal);
    Assert.assertEquals(dirtyState.hashCode(), equal.hashCode());
  }

  @Test
  public void testNotEquals() {
    DirtyState dirtyState = new DirtyState();

    Assert.assertNotEquals(dirtyState, new Object());

    DirtyState differentState = new DirtyState();
    differentState.beforeCommit();
    Assert.assertNotEquals(dirtyState, differentState);
    Assert.assertNotEquals(dirtyState.hashCode(), differentState.hashCode());
  }
}
