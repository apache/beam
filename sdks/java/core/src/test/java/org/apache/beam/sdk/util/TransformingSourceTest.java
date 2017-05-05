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

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.transforms.SerializableFunction;

import com.google.common.collect.Lists;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * Unit tests for {@link TransformingSource}.
 */
@RunWith(JUnit4.class)
public class TransformingSourceTest {

  @SuppressWarnings("deprecation")
  @Test
  public void testTransformingSource() throws Exception {
    int numElements = 10000;
    BoundedSource<Long> longSource = CountingSource.upTo(numElements);
    SerializableFunction<Long, String> toStringFn =
        new SerializableFunction<Long, String>() {
          @Override
          public String apply(Long input) {
            return input.toString();
         }};
    BoundedSource<String> stringSource = new TransformingSource<>(
        longSource, toStringFn, StringUtf8Coder.of());

    List<String> expected = Lists.newArrayList();
    for (int i = 0; i < numElements; i++) {
      expected.add(String.valueOf(i));
    }
    BoundedSourceTester<String> tester = BoundedSourceTester.of(stringSource);
    Assert.assertThat(tester.read(), CoreMatchers.is(expected));
    Assert.assertThat(tester.initSplitThenRead(), CoreMatchers.is(expected));
    Assert.assertThat(tester.readThenDynamicSplit(), CoreMatchers.is(expected));
    Assert.assertThat(tester.initSplitThenReadThenDynamicSplit(), CoreMatchers.is(expected));
  }
}
