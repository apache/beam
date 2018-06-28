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
package org.apache.beam.runners.flink;

import org.apache.beam.runners.flink.translation.types.EncodedValueComparator;
import org.apache.beam.runners.flink.translation.types.EncodedValueTypeInformation;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.ComparatorTestBase;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.junit.Assert;

/** Test for {@link EncodedValueComparator}. */
public class EncodedValueComparatorTest extends ComparatorTestBase<byte[]> {

  @Override
  protected TypeComparator<byte[]> createComparator(boolean ascending) {
    return new EncodedValueTypeInformation().createComparator(ascending, new ExecutionConfig());
  }

  @Override
  protected TypeSerializer<byte[]> createSerializer() {
    return new EncodedValueTypeInformation().createSerializer(new ExecutionConfig());
  }

  @Override
  protected void deepEquals(String message, byte[] should, byte[] is) {
    Assert.assertArrayEquals(message, should, is);
  }

  @Override
  protected byte[][] getSortedTestData() {
    StringUtf8Coder coder = StringUtf8Coder.of();

    try {
      return new byte[][] {
        CoderUtils.encodeToByteArray(coder, ""),
        CoderUtils.encodeToByteArray(coder, "Lorem Ipsum Dolor Omit Longer"),
        CoderUtils.encodeToByteArray(coder, "aaaa"),
        CoderUtils.encodeToByteArray(coder, "abcd"),
        CoderUtils.encodeToByteArray(coder, "abce"),
        CoderUtils.encodeToByteArray(coder, "abdd"),
        CoderUtils.encodeToByteArray(coder, "accd"),
        CoderUtils.encodeToByteArray(coder, "bbcd")
      };
    } catch (CoderException e) {
      throw new RuntimeException("Could not encode values.", e);
    }
  }
}
