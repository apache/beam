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
package org.apache.beam.runners.flink.translation.types.compat;

import java.io.IOException;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/** A legacy snapshot which does not care about schema compatibility. */
@SuppressWarnings("allcheckers")
public class UnversionedTypeSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

  private CoderTypeSerializer<T> serializer = null;

  /** Needs to be public to work with {@link VersionedIOReadableWritable}. */
  public UnversionedTypeSerializerSnapshot() {}

  @SuppressWarnings("initialization")
  public UnversionedTypeSerializerSnapshot(CoderTypeSerializer<T> serializer) {
    this.serializer = serializer;
  }

  @Override
  public int getCurrentVersion() {
    // We always return the same version
    return 1;
  }

  @Override
  public void writeSnapshot(DataOutputView dataOutputView) throws IOException {
    byte[] bytes = SerializableUtils.serializeToByteArray(serializer);
    dataOutputView.writeInt(getCurrentVersion());
    dataOutputView.writeInt(bytes.length);
    dataOutputView.write(bytes);
  }

  @Override
  public void readSnapshot(int i, DataInputView dataInputView, ClassLoader classLoader)
      throws IOException {
    // discard version
    dataInputView.readInt();
    int length = dataInputView.readInt();
    byte[] bytes = new byte[length];
    dataInputView.readFully(bytes);
    this.serializer =
        (CoderTypeSerializer<T>)
            SerializableUtils.deserializeFromByteArray(bytes, "CoderTypeSerializer");
  }

  @Override
  public TypeSerializer<T> restoreSerializer() {
    return serializer;
  }

  @Override
  public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
      TypeSerializer<T> newSerializer) {
    // We assume compatibility because we don't have a way of checking schema compatibility
    return TypeSerializerSchemaCompatibility.compatibleAsIs();
  }
}
