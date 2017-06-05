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
package org.apache.beam.sdk.transforms.join;

import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the UnionCoder.
 */
@RunWith(JUnit4.class)
public class UnionCoderTest {
  @Test
  public void testGetElementCoders() {
    UnionCoder unionCoder =
        UnionCoder.of(ImmutableList.<Coder<?>>of(StringUtf8Coder.of(), DoubleCoder.of()));
    assertThat(
        unionCoder.getElementCoders().get(0), Matchers.<Coder<?>>equalTo(StringUtf8Coder.of()));
    assertThat(unionCoder.getElementCoders().get(1), Matchers.<Coder<?>>equalTo(DoubleCoder.of()));
  }

  @Test
  public void testCoderIsSerializable() {
    CoderProperties.coderSerializable(UnionCoder.of(ImmutableList.<Coder<?>>of(
        StringUtf8Coder.of(), DoubleCoder.of())));
  }
}
