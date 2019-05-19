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
package org.apache.beam.sdk.extensions.smb.tensorflow;

import static org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;

import com.google.protobuf.ByteString;
import java.nio.charset.Charset;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.junit.Assert;
import org.junit.Test;
import org.tensorflow.example.BytesList;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;
import org.tensorflow.example.FloatList;
import org.tensorflow.example.Int64List;

/** Unit tests for {@link TensorFlowMetadata}. */
public class TensorFlowMetadataTest {

  @Test
  public void test() throws Exception {
    final Example example =
        Example.newBuilder()
            .setFeatures(
                Features.newBuilder()
                    .putFeature(
                        "bytes",
                        Feature.newBuilder()
                            .setBytesList(
                                BytesList.newBuilder()
                                    .addValue(ByteString.copyFromUtf8("data"))
                                    .build())
                            .build())
                    .putFeature(
                        "int",
                        Feature.newBuilder()
                            .setInt64List(Int64List.newBuilder().addValue(12345L).build())
                            .build())
                    .putFeature(
                        "float",
                        Feature.newBuilder()
                            .setFloatList(FloatList.newBuilder().addValue(1.2345f).build())
                            .build())
                    .build())
            .build();

    Assert.assertArrayEquals(
        "data".getBytes(Charset.defaultCharset()),
        new TensorFlowMetadata<>(1, 1, byte[].class, HashType.MURMUR3_32, "bytes")
            .extractKey(example));

    Assert.assertEquals(
        (Long) 12345L,
        new TensorFlowMetadata<>(1, 1, Long.class, HashType.MURMUR3_32, "int").extractKey(example));

    Assert.assertThrows(
        NonDeterministicException.class,
        () ->
            new TensorFlowMetadata<>(1, 1, Float.class, HashType.MURMUR3_32, "float")
                .extractKey(example));

    Assert.assertThrows(
        IllegalStateException.class,
        () ->
            new TensorFlowMetadata<>(1, 1, String.class, HashType.MURMUR3_32, "bytes")
                .extractKey(example));
  }
}
