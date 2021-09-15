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
package org.apache.beam.sdk.fn;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link IdGenerators}. */
@RunWith(JUnit4.class)
public class IdGeneratorsTest {
  @Test
  public void incrementing() {
    IdGenerator gen = IdGenerators.incrementingLongs();
    assertThat(gen.getId(), equalTo("1"));
    assertThat(gen.getId(), equalTo("2"));
  }

  @Test
  public void incrementingIndependent() {
    IdGenerator gen = IdGenerators.incrementingLongs();
    IdGenerator otherGen = IdGenerators.incrementingLongs();
    assertThat(gen.getId(), equalTo("1"));
    assertThat(gen.getId(), equalTo("2"));
    assertThat(otherGen.getId(), equalTo("1"));
  }

  @Test
  public void decrementing() {
    IdGenerator gen = IdGenerators.decrementingLongs();
    assertThat(gen.getId(), equalTo("-1"));
    assertThat(gen.getId(), equalTo("-2"));
  }

  @Test
  public void decrementingIndependent() {
    IdGenerator gen = IdGenerators.decrementingLongs();
    IdGenerator otherGen = IdGenerators.decrementingLongs();
    assertThat(gen.getId(), equalTo("-1"));
    assertThat(gen.getId(), equalTo("-2"));
    assertThat(otherGen.getId(), equalTo("-1"));
  }
}
