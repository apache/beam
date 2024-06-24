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
package org.apache.beam.runners.flink.translation.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.Test;

public class UnversionedTypeSerializerSnapshotTest {

  @Test
  public void testSerialization() throws IOException {
    PipelineOptions opts = PipelineOptionsFactory.create();
    CoderTypeSerializer<Integer> serializer =
        new CoderTypeSerializer<>(VarIntCoder.of(), new SerializablePipelineOptions(opts));
    TypeSerializerSnapshot<Integer> snapshot = serializer.snapshotConfiguration();
    assertTrue(snapshot instanceof UnversionedTypeSerializerSnapshot);
    assertEquals(1, snapshot.getCurrentVersion());
    DataOutputSerializer output = new DataOutputSerializer(1);
    snapshot.writeSnapshot(output);
    DataInputDeserializer input = new DataInputDeserializer();
    input.setBuffer(output.wrapAsByteBuffer());
    TypeSerializerSnapshot<Integer> emptySnapshot = new UnversionedTypeSerializerSnapshot<>();
    emptySnapshot.readSnapshot(
        snapshot.getCurrentVersion(), input, Thread.currentThread().getContextClassLoader());
    assertEquals(emptySnapshot.restoreSerializer(), serializer);
  }
}
