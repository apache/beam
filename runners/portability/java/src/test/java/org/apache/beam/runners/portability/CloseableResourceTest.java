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
package org.apache.beam.runners.portability;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.portability.CloseableResource.CloseException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CloseableResource}. */
@RunWith(JUnit4.class)
public class CloseableResourceTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void alwaysReturnsSameResource() {
    Foo foo = new Foo();
    CloseableResource<Foo> resource = CloseableResource.of(foo, ignored -> {});
    assertThat(resource.get(), is(foo));
    assertThat(resource.get(), is(foo));
  }

  @Test
  public void callsCloser() throws Exception {
    AtomicBoolean closed = new AtomicBoolean(false);
    try (CloseableResource<Foo> ignored =
        CloseableResource.of(new Foo(), foo -> closed.set(true))) {
      // Do nothing.
    }
    assertThat(closed.get(), is(true));
  }

  @Test
  public void wrapsExceptionsInCloseException() throws Exception {
    Exception wrapped = new Exception();
    thrown.expect(CloseException.class);
    thrown.expectCause(is(wrapped));
    try (CloseableResource<Foo> ignored =
        CloseableResource.of(
            new Foo(),
            foo -> {
              throw wrapped;
            })) {
      // Do nothing.
    }
  }

  @Test
  public void transferReleasesCloser() throws Exception {
    try (CloseableResource<Foo> foo =
        CloseableResource.of(
            new Foo(), unused -> fail("Transferred resource should not be closed"))) {
      foo.transfer();
    }
  }

  @Test
  public void transferMovesOwnership() throws Exception {
    AtomicBoolean closed = new AtomicBoolean(false);
    CloseableResource<Foo> original = CloseableResource.of(new Foo(), unused -> closed.set(true));
    CloseableResource<Foo> transferred = original.transfer();
    transferred.close();
    assertThat(closed.get(), is(true));
  }

  @Test
  public void cannotTransferClosed() throws Exception {
    CloseableResource<Foo> foo = CloseableResource.of(new Foo(), unused -> {});
    foo.close();
    thrown.expect(IllegalStateException.class);
    foo.transfer();
  }

  @Test
  public void cannotTransferTwice() {
    CloseableResource<Foo> foo = CloseableResource.of(new Foo(), unused -> {});
    foo.transfer();
    thrown.expect(IllegalStateException.class);
    foo.transfer();
  }

  private static class Foo {}
}
