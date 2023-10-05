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

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.DataInputViewWrapper;
import org.apache.beam.runners.flink.translation.wrappers.DataOutputViewWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.EOFException;
import java.io.IOException;

/**
 * Flink {@link org.apache.flink.api.common.typeutils.TypeSerializer} for Beam {@link
 * org.apache.beam.sdk.coders.Coder Coders}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CoderTypeSerializer<T> extends TypeSerializer<T> {

  private final Coder<T> coder;

  /**
   * {@link SerializablePipelineOptions} deserialization will cause {@link
   * org.apache.beam.sdk.io.FileSystems} registration needed for {@link
   * org.apache.beam.sdk.transforms.Reshuffle} translation.
   */
  private final SerializablePipelineOptions pipelineOptions;

  private final boolean fasterCopy;

  public CoderTypeSerializer(Coder<T> coder, SerializablePipelineOptions pipelineOptions) {
    Preconditions.checkNotNull(coder);
    Preconditions.checkNotNull(pipelineOptions);
    this.coder = coder;
    this.pipelineOptions = pipelineOptions;

    FlinkPipelineOptions options = pipelineOptions.get().as(FlinkPipelineOptions.class);
    this.fasterCopy = options.getFasterCopy();
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public CoderTypeSerializer<T> duplicate() {
    return new CoderTypeSerializer<>(coder, pipelineOptions);
  }

  @Override
  public T createInstance() {
    return null;
  }

  @Override
  public T copy(T t) {
    if (fasterCopy) {
      return t;
    }
    try {
      return CoderUtils.clone(coder, t);
    } catch (CoderException e) {
      throw new RuntimeException("Could not clone.", e);
    }
  }

  @Override
  public T copy(T t, T reuse) {
    return copy(t);
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(T t, DataOutputView dataOutputView) throws IOException {
    DataOutputViewWrapper outputWrapper = new DataOutputViewWrapper(dataOutputView);
    coder.encode(t, outputWrapper);
  }

  @Override
  public T deserialize(DataInputView dataInputView) throws IOException {
    try {
      DataInputViewWrapper inputWrapper = new DataInputViewWrapper(dataInputView);
      return coder.decode(inputWrapper);
    } catch (CoderException e) {
      Throwable cause = e.getCause();
      if (cause instanceof EOFException) {
        throw (EOFException) cause;
      } else {
        throw e;
      }
    }
  }

  @Override
  public T deserialize(T t, DataInputView dataInputView) throws IOException {
    return deserialize(dataInputView);
  }

  @Override
  public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
    serialize(deserialize(dataInputView), dataOutputView);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CoderTypeSerializer that = (CoderTypeSerializer) o;
    return coder.equals(that.coder);
  }

  @Override
  public int hashCode() {
    return coder.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<T> snapshotConfiguration() {
    return new LegacySnapshot<>(this);
  }

  public static class LegacySnapshot<T> implements TypeSerializerSnapshot<T> {

    int CURRENT_VERSION = 2;
    CoderTypeSerializer<T> serializer;

    public LegacySnapshot() {
    }

    public LegacySnapshot(CoderTypeSerializer<T> serializer) {
      this.serializer = serializer;
    }

    @Override
    public int getCurrentVersion() {
      return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
      ByteArrayOutputStreamWithPos streamWithPos = new ByteArrayOutputStreamWithPos();
      InstantiationUtil.serializeObject(streamWithPos, this.serializer);
      out.writeInt(streamWithPos.getPosition());
      out.write(streamWithPos.getBuf(), 0, streamWithPos.getPosition());
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
      switch (readVersion) {
        case 1:
          throw new UnsupportedOperationException(
                  String.format("No longer supported version [%d].", readVersion));
        case 2:
          try {
            int serializerBytes = in.readInt();
            byte[] buffer = new byte[serializerBytes];
            in.readFully(buffer);
            this.serializer = InstantiationUtil.deserializeObject(buffer, userCodeClassLoader);
          } catch (ClassNotFoundException e) {
            throw new IOException(e);
          }
          break;
        default:
          throw new IllegalArgumentException("Unrecognized version: " + readVersion);
      }
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
      if (serializer == null) {
        throw new IllegalStateException(
                "Trying to restore the prior serializer but the prior serializer has not been set.");
      }
      return this.serializer;
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
      if (newSerializer.getClass() != this.getClass().getDeclaringClass()) {
        return TypeSerializerSchemaCompatibility.incompatible();
      }

      CoderTypeSerializer<T> coderTypeSerializer = (CoderTypeSerializer<T>) newSerializer;

      if (this.serializer == null) {
        return TypeSerializerSchemaCompatibility.incompatible();
      }

      if (!this.serializer.coder.getEncodedTypeDescriptor().getType().getTypeName().equals(coderTypeSerializer.coder.getEncodedTypeDescriptor().getType().getTypeName())) {
        return TypeSerializerSchemaCompatibility.incompatible();
      }

      if (!this.serializer.pipelineOptions.toString().equals(coderTypeSerializer.pipelineOptions.toString())) {
        return TypeSerializerSchemaCompatibility.incompatible();
      }

      return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }
  }

  @Override
  public String toString() {
    return "CoderTypeSerializer{" + "coder=" + coder + '}';
  }
}
