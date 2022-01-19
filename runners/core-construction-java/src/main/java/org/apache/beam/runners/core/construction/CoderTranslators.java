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
package org.apache.beam.runners.core.construction;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.model.pipeline.v1.SchemaApi.SchemaCoderPayload;
import org.apache.beam.runners.core.construction.CoderTranslation.TranslationContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.TimestampPrefixingWindowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.vendor.grpc.v1p43p2.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** {@link CoderTranslator} implementations for known coder types. */
class CoderTranslators {
  private CoderTranslators() {}

  static <T extends Coder<?>> CoderTranslator<T> atomic(final Class<T> clazz) {
    return new SimpleStructuredCoderTranslator<T>() {
      @Override
      public List<? extends Coder<?>> getComponents(T from) {
        return Collections.emptyList();
      }

      @Override
      public T fromComponents(List<Coder<?>> components) {
        return InstanceBuilder.ofType(clazz).build();
      }
    };
  }

  static CoderTranslator<KvCoder<?, ?>> kv() {
    return new SimpleStructuredCoderTranslator<KvCoder<?, ?>>() {
      @Override
      public List<? extends Coder<?>> getComponents(KvCoder<?, ?> from) {
        return ImmutableList.of(from.getKeyCoder(), from.getValueCoder());
      }

      @Override
      public KvCoder<?, ?> fromComponents(List<Coder<?>> components) {
        return KvCoder.of(components.get(0), components.get(1));
      }
    };
  }

  static CoderTranslator<IterableCoder<?>> iterable() {
    return new SimpleStructuredCoderTranslator<IterableCoder<?>>() {
      @Override
      public List<? extends Coder<?>> getComponents(IterableCoder<?> from) {
        return Collections.singletonList(from.getElemCoder());
      }

      @Override
      public IterableCoder<?> fromComponents(List<Coder<?>> components) {
        return IterableCoder.of(components.get(0));
      }
    };
  }

  static CoderTranslator<Timer.Coder<?>> timer() {
    return new SimpleStructuredCoderTranslator<Timer.Coder<?>>() {
      @Override
      public List<? extends Coder<?>> getComponents(Timer.Coder<?> from) {
        return from.getComponents();
      }

      @Override
      public Timer.Coder<?> fromComponents(List<Coder<?>> components) {
        return Timer.Coder.of(components.get(0), (Coder<BoundedWindow>) components.get(1));
      }
    };
  }

  static CoderTranslator<LengthPrefixCoder<?>> lengthPrefix() {
    return new SimpleStructuredCoderTranslator<LengthPrefixCoder<?>>() {
      @Override
      public List<? extends Coder<?>> getComponents(LengthPrefixCoder<?> from) {
        return Collections.singletonList(from.getValueCoder());
      }

      @Override
      public LengthPrefixCoder<?> fromComponents(List<Coder<?>> components) {
        return LengthPrefixCoder.of(components.get(0));
      }
    };
  }

  static CoderTranslator<FullWindowedValueCoder<?>> fullWindowedValue() {
    return new SimpleStructuredCoderTranslator<FullWindowedValueCoder<?>>() {
      @Override
      public List<? extends Coder<?>> getComponents(FullWindowedValueCoder<?> from) {
        return ImmutableList.of(from.getValueCoder(), from.getWindowCoder());
      }

      @Override
      public FullWindowedValueCoder<?> fromComponents(List<Coder<?>> components) {
        return WindowedValue.getFullCoder(
            components.get(0), (Coder<BoundedWindow>) components.get(1));
      }
    };
  }

  static CoderTranslator<WindowedValue.ParamWindowedValueCoder<?>> paramWindowedValue() {
    return new CoderTranslator<WindowedValue.ParamWindowedValueCoder<?>>() {
      @Override
      public List<? extends Coder<?>> getComponents(WindowedValue.ParamWindowedValueCoder<?> from) {
        return ImmutableList.of(from.getValueCoder(), from.getWindowCoder());
      }

      @Override
      public byte[] getPayload(WindowedValue.ParamWindowedValueCoder<?> from) {
        return WindowedValue.ParamWindowedValueCoder.getPayload(from);
      }

      @Override
      public WindowedValue.ParamWindowedValueCoder<?> fromComponents(
          List<Coder<?>> components, byte[] payload, TranslationContext context) {
        return WindowedValue.ParamWindowedValueCoder.fromComponents(components, payload);
      }
    };
  }

  static CoderTranslator<RowCoder> rowV1() {
    return new CoderTranslator<RowCoder>() {
      @Override
      public List<? extends Coder<?>> getComponents(RowCoder from) {
        return ImmutableList.of();
      }

      @Override
      public byte[] getPayload(RowCoder from) {
        return SchemaTranslation.schemaToProto(from.getSchema(), true).toByteArray();
      }

      @Override
      public RowCoder fromComponents(
          List<Coder<?>> components, byte[] payload, TranslationContext context) {
        checkArgument(
            components.isEmpty(), "Expected empty component list, but received: " + components);
        Schema schema;
        try {
          schema = SchemaTranslation.schemaFromProto(SchemaApi.Schema.parseFrom(payload));
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Unable to parse schema for RowCoder: ", e);
        }
        return RowCoder.of(schema);
      }
    };
  }

  static CoderTranslator<RowCoder> row() {
    return new CoderTranslator<RowCoder>() {
      @Override
      public List<? extends Coder<?>> getComponents(RowCoder from) {
        return ImmutableList.of();
      }

      @Override
      public byte[] getPayload(RowCoder from) {
        SchemaApi.SchemaCoderPayload.Builder coderBuilder =
            SchemaApi.SchemaCoderPayload.newBuilder();
        coderBuilder.setSchema(SchemaTranslation.schemaToProto(from.getSchema(), true));
        return coderBuilder.build().toByteArray();
      }

      @Override
      public RowCoder fromComponents(
          List<Coder<?>> components, byte[] payload, TranslationContext context) {
        checkArgument(
            components.isEmpty(), "Expected empty component list, but received: " + components);
        Schema schema;
        try {
          schema =
              SchemaTranslation.schemaFromProto(
                  SchemaApi.SchemaCoderPayload.parseFrom(payload).getSchema());
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Unable to parse schema for RowCoder: ", e);
        }
        return RowCoder.of(schema);
      }
    };
  }

  private static final String TYPE_DESCRIPTOR = "typeDescriptor";
  private static final String FROM_ROW_FUNCTION = "fromRow";
  private static final String TO_ROW_FUNCTION = "toRow";

  public static boolean isSchemaCoder(RunnerApi.Coder coder) {
    return isSchemaCoder(coder.getSpec().getUrn(), coder.getSpec().getPayload().toByteArray());
  }

  public static boolean isSchemaCoder(String urn, byte[] payload) {
    if (!urn.equals(ModelCoders.ROW_CODER_URN)) {
      return false;
    }
    try {
      SchemaCoderPayload parsedPayload = SchemaApi.SchemaCoderPayload.parseFrom(payload);
      return parsedPayload.getSdkInformationMap().containsKey(TYPE_DESCRIPTOR);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Unable to parse schema for RowCoder: ", e);
    }
  }

  static <T> CoderTranslator<SchemaCoder<T>> schema() {
    return new CoderTranslator<SchemaCoder<T>>() {
      @Override
      public List<? extends Coder<?>> getComponents(SchemaCoder<T> from) {
        return ImmutableList.of();
      }

      @Override
      public byte[] getPayload(SchemaCoder<T> from) {
        SchemaApi.SchemaCoderPayload.Builder coderBuilder =
            SchemaApi.SchemaCoderPayload.newBuilder();
        coderBuilder.setSchema(SchemaTranslation.schemaToProto(from.getSchema(), true));
        coderBuilder.putSdkInformation(
            TYPE_DESCRIPTOR,
            ByteString.copyFrom(
                SerializableUtils.serializeToByteArray(from.getEncodedTypeDescriptor())));
        coderBuilder.putSdkInformation(
            FROM_ROW_FUNCTION,
            ByteString.copyFrom(SerializableUtils.serializeToByteArray(from.getFromRowFunction())));
        coderBuilder.putSdkInformation(
            TO_ROW_FUNCTION,
            ByteString.copyFrom(SerializableUtils.serializeToByteArray(from.getToRowFunction())));

        return coderBuilder.build().toByteArray();
      }

      @Override
      public SchemaCoder<T> fromComponents(
          List<Coder<?>> components, byte[] payload, TranslationContext context) {
        checkArgument(
            components.isEmpty(), "Expected empty component list, but received: " + components);
        try {
          SchemaCoderPayload parsedPayload = SchemaApi.SchemaCoderPayload.parseFrom(payload);
          Schema schema = SchemaTranslation.schemaFromProto(parsedPayload.getSchema());
          TypeDescriptor<T> typeDescriptor =
              (TypeDescriptor<T>)
                  SerializableUtils.deserializeFromByteArray(
                      parsedPayload.getSdkInformationMap().get(TYPE_DESCRIPTOR).toByteArray(),
                      "typeDescriptor");
          SerializableFunction<T, Row> toRowFunction =
              (SerializableFunction<T, Row>)
                  SerializableUtils.deserializeFromByteArray(
                      parsedPayload.getSdkInformationMap().get(TO_ROW_FUNCTION).toByteArray(),
                      "toRowFunction");
          SerializableFunction<Row, T> fromRowFunction =
              (SerializableFunction<Row, T>)
                  SerializableUtils.deserializeFromByteArray(
                      parsedPayload.getSdkInformationMap().get(FROM_ROW_FUNCTION).toByteArray(),
                      "fromRowFunction");

          return SchemaCoder.of(schema, typeDescriptor, toRowFunction, fromRowFunction);
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Unable to parse schema for SchemaCoder: ", e);
        }
      }
    };
  }

  static CoderTranslator<ShardedKey.Coder<?>> shardedKey() {
    return new SimpleStructuredCoderTranslator<ShardedKey.Coder<?>>() {
      @Override
      public List<? extends Coder<?>> getComponents(ShardedKey.Coder<?> from) {
        return Collections.singletonList(from.getKeyCoder());
      }

      @Override
      public ShardedKey.Coder<?> fromComponents(List<Coder<?>> components) {
        return ShardedKey.Coder.of(components.get(0));
      }
    };
  }

  static CoderTranslator<TimestampPrefixingWindowCoder<?>> timestampPrefixingWindow() {
    return new SimpleStructuredCoderTranslator<TimestampPrefixingWindowCoder<?>>() {
      @Override
      protected TimestampPrefixingWindowCoder<?> fromComponents(List<Coder<?>> components) {
        return TimestampPrefixingWindowCoder.of((Coder<? extends BoundedWindow>) components.get(0));
      }

      @Override
      public List<? extends Coder<?>> getComponents(TimestampPrefixingWindowCoder<?> from) {
        return from.getComponents();
      }
    };
  }

  public abstract static class SimpleStructuredCoderTranslator<T extends Coder<?>>
      implements CoderTranslator<T> {
    @Override
    public final T fromComponents(
        List<Coder<?>> components, byte[] payload, TranslationContext context) {
      return fromComponents(components);
    }

    protected abstract T fromComponents(List<Coder<?>> components);
  }
}
