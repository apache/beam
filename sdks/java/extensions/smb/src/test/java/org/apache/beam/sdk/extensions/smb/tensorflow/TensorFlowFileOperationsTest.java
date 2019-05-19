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

import static org.apache.beam.sdk.extensions.smb.TestUtils.fromFolder;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.extensions.smb.FileOperations;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tensorflow.example.BytesList;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;
import org.tensorflow.example.FloatList;
import org.tensorflow.example.Int64List;

/** Unit tests for {@link TensorFlowFileOperations}. */
public class TensorFlowFileOperationsTest {
  @Rule public final TemporaryFolder output = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    final TensorFlowFileOperations fileOperations = new TensorFlowFileOperations();
    final ResourceId file =
        fromFolder(output)
            .resolve("file.tfrecords", ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

    final List<Example> records =
        IntStream.range(0, 10)
            .mapToObj(
                i ->
                    Example.newBuilder()
                        .setFeatures(
                            Features.newBuilder()
                                .putFeature(
                                    "bytes",
                                    Feature.newBuilder()
                                        .setBytesList(
                                            BytesList.newBuilder()
                                                .addValue(ByteString.copyFromUtf8("bytes-" + i))
                                                .build())
                                        .build())
                                .putFeature(
                                    "int",
                                    Feature.newBuilder()
                                        .setInt64List(Int64List.newBuilder().addValue(i).build())
                                        .build())
                                .putFeature(
                                    "float",
                                    Feature.newBuilder()
                                        .setFloatList(FloatList.newBuilder().addValue(i).build())
                                        .build())
                                .build())
                        .build())
            .collect(Collectors.toList());
    final FileOperations.Writer<Example> writer = fileOperations.createWriter();
    writer.prepareWrite(FileSystems.create(file, writer.getMimeType()));
    for (Example record : records) {
      writer.write(record);
    }
    writer.close();

    final List<Example> actual = new ArrayList<>();
    fileOperations.iterator(file).forEachRemaining(actual::add);

    Assert.assertEquals(records, actual);
  }
}
