package org.apache.beam.runners.jstorm.serialization;

import backtype.storm.Config;
import com.alibaba.jstorm.esotericsoftware.kryo.Kryo;
import com.alibaba.jstorm.esotericsoftware.kryo.Serializer;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Input;
import com.alibaba.jstorm.esotericsoftware.kryo.io.Output;
import org.apache.beam.sdk.repackaged.com.google.common.collect.ImmutableSet;
import org.apache.beam.sdk.repackaged.com.google.common.collect.Sets;

public class SdkRepackImmuSetSerializer extends Serializer<ImmutableSet<Object>> {

  private static final boolean DOES_NOT_ACCEPT_NULL = false;
  private static final boolean IMMUTABLE = true;

  public SdkRepackImmuSetSerializer() {
    super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
  }

  @Override
  public void write(Kryo kryo, Output output, ImmutableSet<Object> object) {
    output.writeInt(object.size(), true);
    for (Object elm : object) {
      kryo.writeClassAndObject(output, elm);
    }
  }

  @Override
  public ImmutableSet<Object> read(Kryo kryo, Input input, Class<ImmutableSet<Object>> type) {
    final int size = input.readInt(true);
    ImmutableSet.Builder<Object> builder = ImmutableSet.builder();
    for (int i = 0; i < size; ++i) {
      builder.add(kryo.readClassAndObject(input));
    }
    return builder.build();
  }

  /**
   * Creates a new {@link ImmutableSetSerializer} and registers its serializer
   * for the several ImmutableSet related classes.
   */
  public static void registerSerializers(Config config) {

    // ImmutableList (abstract class)
    //  +- EmptyImmutableSet
    //  |   EmptyImmutableSet
    //  +- SingletonImmutableSet
    //  |   Optimized for Set with only 1 element.
    //  +- RegularImmutableSet
    //  |   RegularImmutableList
    //  +- EnumImmutableSet
    //  |   EnumImmutableSet

    config.registerSerialization(ImmutableSet.class, SdkRepackImmuSetSerializer.class);

    // Note:
    //  Only registering above is good enough for serializing/deserializing.
    //  but if using Kryo#copy, following is required.

    config.registerSerialization(ImmutableSet.of().getClass(), SdkRepackImmuSetSerializer.class);
    config.registerSerialization(ImmutableSet.of(1).getClass(), SdkRepackImmuSetSerializer.class);
    config.registerSerialization(
        ImmutableSet.of(1, 2, 3).getClass(),
        SdkRepackImmuSetSerializer.class);

    config.registerSerialization(
        Sets.immutableEnumSet(SomeEnum.A, SomeEnum.B, SomeEnum.C).getClass(),
        SdkRepackImmuSetSerializer.class);
  }

  private enum SomeEnum {
    A, B, C
  }
}

