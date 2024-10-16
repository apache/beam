package org.apache.beam.runners.flink.adapter;

import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.runners.flink.translation.wrappers.streaming.FlinkKeyUtils;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

public class FlinkKey implements Value {

  private final CoderTypeSerializer<byte[]> serializer;

  @SuppressWarnings("initialization.fields.uninitialized")
  private ByteBuffer underlying;

  public FlinkKey() {
    this.serializer = new CoderTypeSerializer<>(ByteArrayCoder.of(), false);
  }

  private FlinkKey(ByteBuffer underlying) {
    this();
    this.underlying = underlying;
  }

  public ByteBuffer getSerializedKey() {
    return underlying;
  }

  public static FlinkKey of(ByteBuffer bytes) {
    return new FlinkKey(bytes);
  }

  public static <K> FlinkKey of(K key, Coder<K> coder) {
    return new FlinkKey(FlinkKeyUtils.encodeKey(key, coder));
  }

  @Override
  public void write(DataOutputView out) throws IOException {
    checkNotNull(underlying);
    serializer.serialize(underlying.array(), out);
  }

  @Override
  public void read(DataInputView in) throws IOException {
    this.underlying = ByteBuffer.wrap(serializer.deserialize(in));
  }

  public <K> K getKey(Coder<K> coder) {
    return FlinkKeyUtils.decodeKey(underlying, coder);
  }

  @Override
  public int hashCode() {
//    return underlying.hashCode();
    return Hashing.murmur3_128().hashBytes(underlying.array()).asInt();
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    return obj instanceof FlinkKey && ((FlinkKey) obj).underlying.equals(underlying);
  }
}
