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

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.DataInputViewWrapper;
import org.apache.beam.runners.flink.translation.wrappers.DataOutputViewWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Flink {@link org.apache.flink.api.common.typeutils.TypeSerializer} for Beam {@link
 * org.apache.beam.sdk.coders.Coder Coders}.
 */
public class CoderTypeSerializer<T> extends TypeSerializer<T> {

  private final Coder<T> coder;

  /**
   * {@link SerializablePipelineOptions} deserialization will cause {@link
   * org.apache.beam.sdk.io.FileSystems} registration needed for {@link
   * org.apache.beam.sdk.transforms.Reshuffle} translation.
   */
  @SuppressWarnings("unused")
  @Nullable
  private final SerializablePipelineOptions pipelineOptions;

  public CoderTypeSerializer(Coder<T> coder) {
    Preconditions.checkNotNull(coder);
    this.coder = coder;
    this.pipelineOptions = null;
  }

  public CoderTypeSerializer(
      Coder<T> coder, @Nullable SerializablePipelineOptions pipelineOptions) {
    Preconditions.checkNotNull(coder);
    this.coder = coder;
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public CoderTypeSerializer<T> duplicate() {
    return new CoderTypeSerializer<>(coder);
  }

  @Override
  public T createInstance() {
    return null;
  }

  @Override
  public T copy(T t) {
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
  public boolean equals(Object o) {
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
  public boolean canEqual(Object obj) {
    return obj instanceof CoderTypeSerializer;
  }

  @Override
  public int hashCode() {
    return coder.hashCode();
  }

  @Override
  public TypeSerializerConfigSnapshot snapshotConfiguration() {
    return new CoderTypeSerializerConfigSnapshot<>(coder);
  }

  @Override
  public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
    if (snapshotConfiguration().equals(configSnapshot)) {
      return CompatibilityResult.compatible();
    }
    return CompatibilityResult.requiresMigration();
  }

  /**
   * TypeSerializerConfigSnapshot of CoderTypeSerializer. This uses the class name of the {@link
   * Coder} to determine compatibility. This is a bit crude but better than using Java Serialization
   * to (de)serialize the {@link Coder}.
   */
  public static class CoderTypeSerializerConfigSnapshot<T> extends TypeSerializerConfigSnapshot {

    private static final int VERSION = 1;
    private String coderName;

    public CoderTypeSerializerConfigSnapshot() {
      // empty constructor for satisfying IOReadableWritable which is used for deserialization
    }

    public CoderTypeSerializerConfigSnapshot(Coder<T> coder) {
      this.coderName = coder.getClass().getName();
    }

    @Override
    public int getVersion() {
      return VERSION;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CoderTypeSerializerConfigSnapshot<?> that = (CoderTypeSerializerConfigSnapshot<?>) o;

      return coderName != null ? coderName.equals(that.coderName) : that.coderName == null;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
      super.write(out);
      out.writeUTF(coderName);
    }

    @Override
    public void read(DataInputView in) throws IOException {
      super.read(in);
      this.coderName = in.readUTF();
    }

    @Override
    public int hashCode() {
      return Objects.hash(coderName);
    }
  }

  @Override
  public String toString() {
    return "CoderTypeSerializer{" + "coder=" + coder + '}';
  }
}
