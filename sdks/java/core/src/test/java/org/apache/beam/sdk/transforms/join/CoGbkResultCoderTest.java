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
package com.google.cloud.dataflow.sdk.transforms.join;

import static org.junit.Assert.assertFalse;

import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DoubleCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult.CoGbkResultCoder;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the CoGbkResult.CoGbkResultCoder.
 */
@RunWith(JUnit4.class)
public class CoGbkResultCoderTest {

  private static final CoGbkResultSchema TEST_SCHEMA =
        new CoGbkResultSchema(TupleTagList.of(new TupleTag<String>()).and(
            new TupleTag<Integer>()));

  private static final UnionCoder TEST_UNION_CODER =
      UnionCoder.of(ImmutableList.<Coder<?>>of(
          StringUtf8Coder.of(),
          VarIntCoder.of()));

  private static final UnionCoder COMPATIBLE_UNION_CODER =
      UnionCoder.of(ImmutableList.<Coder<?>>of(
          StringUtf8Coder.of(),
          BigEndianIntegerCoder.of()));

  private static final CoGbkResultSchema INCOMPATIBLE_SCHEMA =
        new CoGbkResultSchema(TupleTagList.of(new TupleTag<String>()).and(
            new TupleTag<Double>()));

  private static final UnionCoder INCOMPATIBLE_UNION_CODER =
      UnionCoder.of(ImmutableList.<Coder<?>>of(
          StringUtf8Coder.of(),
          DoubleCoder.of()));

  private static final CoGbkResultCoder TEST_CODER =
      CoGbkResultCoder.of(TEST_SCHEMA, TEST_UNION_CODER);

  private static final CoGbkResultCoder COMPATIBLE_TEST_CODER =
      CoGbkResultCoder.of(TEST_SCHEMA, COMPATIBLE_UNION_CODER);

  private static final CoGbkResultCoder INCOMPATIBLE_TEST_CODER =
      CoGbkResultCoder.of(INCOMPATIBLE_SCHEMA, INCOMPATIBLE_UNION_CODER);

  @Test
  public void testEquals() {
    assertFalse(TEST_CODER.equals(new Object()));
    assertFalse(TEST_CODER.equals(COMPATIBLE_TEST_CODER));
    assertFalse(TEST_CODER.equals(INCOMPATIBLE_TEST_CODER));
  }

  @Test
  public void testSerializationDeserialization() {
    CoderProperties.coderSerializable(TEST_CODER);
  }
}
