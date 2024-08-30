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
package org.apache.beam.runners.dataflow.util;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.IterableLikeCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.TimestampPrefixingWindowCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.join.CoGbkResult.CoGbkResultCoder;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Utilities for creating {@link CloudObjectTranslator} instances for {@link Coder Coders}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class CloudObjectTranslators {
  private CloudObjectTranslators() {}

  private static CloudObject addComponents(
      CloudObject base, List<? extends Coder<?>> components, SdkComponents sdkComponents) {
    if (!components.isEmpty()) {
      List<CloudObject> cloudComponents = new ArrayList<>(components.size());
      for (Coder component : components) {
        cloudComponents.add(CloudObjects.asCloudObject(component, sdkComponents));
      }
      Structs.addList(base, PropertyNames.COMPONENT_ENCODINGS, cloudComponents);
    }

    return base;
  }

  private static List<Coder<?>> getComponents(CloudObject target) {
    List<Map<String, Object>> cloudComponents =
        Structs.getListOfMaps(target, PropertyNames.COMPONENT_ENCODINGS, Collections.emptyList());
    List<Coder<?>> components = new ArrayList<>();
    for (Map<String, Object> cloudComponent : cloudComponents) {
      components.add(CloudObjects.coderFromCloudObject(CloudObject.fromSpec(cloudComponent)));
    }
    return components;
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "pair".
   */
  public static CloudObjectTranslator<KvCoder> pair() {
    return new CloudObjectTranslator<KvCoder>() {
      @Override
      public CloudObject toCloudObject(KvCoder target, SdkComponents sdkComponents) {
        CloudObject result = CloudObject.forClassName(CloudObjectKinds.KIND_PAIR);
        Structs.addBoolean(result, PropertyNames.IS_PAIR_LIKE, true);
        return addComponents(
            result,
            ImmutableList.<Coder<?>>of(target.getKeyCoder(), target.getValueCoder()),
            sdkComponents);
      }

      @Override
      public KvCoder fromCloudObject(CloudObject object) {
        List<Coder<?>> components = getComponents(object);
        checkArgument(components.size() == 2, "Expecting 2 components, got %s", components.size());
        return KvCoder.of(components.get(0), components.get(1));
      }

      @Override
      public Class<KvCoder> getSupportedClass() {
        return KvCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObjectKinds.KIND_PAIR;
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "stream".
   */
  public static CloudObjectTranslator<IterableCoder> stream() {
    return new CloudObjectTranslator<IterableCoder>() {
      @Override
      public CloudObject toCloudObject(IterableCoder target, SdkComponents sdkComponents) {
        CloudObject result = CloudObject.forClassName(CloudObjectKinds.KIND_STREAM);
        Structs.addBoolean(result, PropertyNames.IS_STREAM_LIKE, true);
        return addComponents(
            result, Collections.<Coder<?>>singletonList(target.getElemCoder()), sdkComponents);
      }

      @Override
      public IterableCoder fromCloudObject(CloudObject object) {
        List<Coder<?>> components = getComponents(object);
        checkArgument(components.size() == 1, "Expecting 1 component, got %s", components.size());
        return IterableCoder.of(components.get(0));
      }

      @Override
      public Class<? extends IterableCoder> getSupportedClass() {
        return IterableCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObjectKinds.KIND_STREAM;
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "length_prefix".
   */
  static CloudObjectTranslator<LengthPrefixCoder> lengthPrefix() {
    return new CloudObjectTranslator<LengthPrefixCoder>() {
      @Override
      public CloudObject toCloudObject(LengthPrefixCoder target, SdkComponents sdkComponents) {
        return addComponents(
            CloudObject.forClassName(CloudObjectKinds.KIND_LENGTH_PREFIX),
            Collections.<Coder<?>>singletonList(target.getValueCoder()),
            sdkComponents);
      }

      @Override
      public LengthPrefixCoder fromCloudObject(CloudObject object) {
        List<Coder<?>> components = getComponents(object);
        checkArgument(components.size() == 1, "Expecting 1 component, got %s", components.size());
        return LengthPrefixCoder.of(components.get(0));
      }

      @Override
      public Class<? extends LengthPrefixCoder> getSupportedClass() {
        return LengthPrefixCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObjectKinds.KIND_LENGTH_PREFIX;
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "global_window".
   */
  static CloudObjectTranslator<GlobalWindow.Coder> globalWindow() {
    return new CloudObjectTranslator<GlobalWindow.Coder>() {
      @Override
      public CloudObject toCloudObject(GlobalWindow.Coder target, SdkComponents sdkComponents) {
        return addComponents(
            CloudObject.forClassName(CloudObjectKinds.KIND_GLOBAL_WINDOW),
            Collections.emptyList(),
            sdkComponents);
      }

      @Override
      public GlobalWindow.Coder fromCloudObject(CloudObject object) {
        return GlobalWindow.Coder.INSTANCE;
      }

      @Override
      public Class<? extends GlobalWindow.Coder> getSupportedClass() {
        return GlobalWindow.Coder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObjectKinds.KIND_GLOBAL_WINDOW;
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "interval_window".
   */
  static CloudObjectTranslator<IntervalWindowCoder> intervalWindow() {
    return new CloudObjectTranslator<IntervalWindowCoder>() {
      @Override
      public CloudObject toCloudObject(IntervalWindowCoder target, SdkComponents sdkComponents) {
        return addComponents(
            CloudObject.forClassName(CloudObjectKinds.KIND_INTERVAL_WINDOW),
            Collections.emptyList(),
            sdkComponents);
      }

      @Override
      public IntervalWindowCoder fromCloudObject(CloudObject object) {
        return IntervalWindowCoder.of();
      }

      @Override
      public Class<? extends IntervalWindowCoder> getSupportedClass() {
        return IntervalWindowCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObjectKinds.KIND_INTERVAL_WINDOW;
      }
    };
  }

  static CloudObjectTranslator<TimestampPrefixingWindowCoder> customWindow() {
    return new CloudObjectTranslator<TimestampPrefixingWindowCoder>() {
      @Override
      public CloudObject toCloudObject(
          TimestampPrefixingWindowCoder target, SdkComponents sdkComponents) {
        CloudObject result = CloudObject.forClassName(CloudObjectKinds.KIND_CUSTOM_WINDOW);
        return addComponents(result, target.getComponents(), sdkComponents);
      }

      @Override
      public TimestampPrefixingWindowCoder fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> components = getComponents(cloudObject);
        checkArgument(components.size() == 1, "Expecting 1 component, got %s", components.size());
        return TimestampPrefixingWindowCoder.of((Coder<? extends BoundedWindow>) components.get(0));
      }

      @Override
      public Class<? extends TimestampPrefixingWindowCoder> getSupportedClass() {
        return TimestampPrefixingWindowCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObjectKinds.KIND_CUSTOM_WINDOW;
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "windowed_value".
   */
  static CloudObjectTranslator<FullWindowedValueCoder> windowedValue() {
    return new CloudObjectTranslator<FullWindowedValueCoder>() {
      @Override
      public CloudObject toCloudObject(FullWindowedValueCoder target, SdkComponents sdkComponents) {
        CloudObject result = CloudObject.forClassName(CloudObjectKinds.KIND_WINDOWED_VALUE);
        Structs.addBoolean(result, PropertyNames.IS_WRAPPER, true);
        return addComponents(
            result,
            ImmutableList.<Coder<?>>of(target.getValueCoder(), target.getWindowCoder()),
            sdkComponents);
      }

      @Override
      public FullWindowedValueCoder fromCloudObject(CloudObject object) {
        List<Coder<?>> components = getComponents(object);
        checkArgument(components.size() == 2, "Expecting 2 components, got " + components.size());
        @SuppressWarnings("unchecked")
        Coder<? extends BoundedWindow> window = (Coder<? extends BoundedWindow>) components.get(1);
        return FullWindowedValueCoder.of(components.get(0), window);
      }

      @Override
      public Class<? extends FullWindowedValueCoder> getSupportedClass() {
        return FullWindowedValueCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObjectKinds.KIND_WINDOWED_VALUE;
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "bytes".
   */
  static CloudObjectTranslator<ByteArrayCoder> bytes() {
    return new CloudObjectTranslator<ByteArrayCoder>() {
      @Override
      public CloudObject toCloudObject(ByteArrayCoder target, SdkComponents sdkComponents) {
        return addComponents(
            CloudObject.forClassName(CloudObjectKinds.KIND_BYTES),
            Collections.emptyList(),
            sdkComponents);
      }

      @Override
      public ByteArrayCoder fromCloudObject(CloudObject object) {
        return ByteArrayCoder.of();
      }

      @Override
      public Class<? extends ByteArrayCoder> getSupportedClass() {
        return ByteArrayCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObjectKinds.KIND_BYTES;
      }
    };
  }

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "varint".
   */
  static CloudObjectTranslator<VarLongCoder> varInt() {
    return new CloudObjectTranslator<VarLongCoder>() {
      @Override
      public CloudObject toCloudObject(VarLongCoder target, SdkComponents sdkComponents) {
        return addComponents(
            CloudObject.forClass(target.getClass()), Collections.emptyList(), sdkComponents);
      }

      @Override
      public VarLongCoder fromCloudObject(CloudObject object) {
        return VarLongCoder.of();
      }

      @Override
      public Class<? extends VarLongCoder> getSupportedClass() {
        return VarLongCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObject.forClass(VarLongCoder.class).getClassName();
      }
    };
  }

  private static final String CODER_FIELD = "serialized_coder";
  private static final String TYPE_FIELD = "type";

  public static CloudObjectTranslator<Coder> javaSerialized() {
    return new CloudObjectTranslator<Coder>() {
      @Override
      public CloudObject toCloudObject(Coder target, SdkComponents sdkComponents) {
        // CustomCoder is used as the "marker" for a java-serialized coder
        CloudObject cloudObject = CloudObject.forClass(CustomCoder.class);
        Structs.addString(cloudObject, TYPE_FIELD, target.getClass().getName());
        Structs.addString(
            cloudObject,
            CODER_FIELD,
            StringUtils.byteArrayToJsonString(SerializableUtils.serializeToByteArray(target)));
        return cloudObject;
      }

      @Override
      public Coder fromCloudObject(CloudObject cloudObject) {
        String serializedCoder = Structs.getString(cloudObject, CODER_FIELD);
        String type = Structs.getString(cloudObject, TYPE_FIELD);
        return (Coder<?>)
            SerializableUtils.deserializeFromByteArray(
                StringUtils.jsonStringToByteArray(serializedCoder), type);
      }

      @Override
      public Class<? extends CustomCoder> getSupportedClass() {
        return CustomCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObject.forClass(CustomCoder.class).getClassName();
      }
    };
  }

  public static <T extends Coder> CloudObjectTranslator<T> atomic(final Class<T> coderClass) {
    // Make sure that the instance will be instantiable from the class.
    InstanceBuilder.ofType(coderClass).fromFactoryMethod("of").build();
    return new CloudObjectTranslator<T>() {
      @Override
      public CloudObject toCloudObject(T target, SdkComponents sdkComponents) {
        return CloudObject.forClass(coderClass);
      }

      @Override
      public T fromCloudObject(CloudObject cloudObject) {
        return InstanceBuilder.ofType(coderClass).fromFactoryMethod("of").build();
      }

      @Override
      public Class<? extends T> getSupportedClass() {
        return coderClass;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObject.forClass(coderClass).getClassName();
      }
    };
  }

  public static CloudObjectTranslator<IterableLikeCoder> iterableLike(
      final Class<? extends IterableLikeCoder> clazz) {
    return new CloudObjectTranslator<IterableLikeCoder>() {
      @Override
      public CloudObject toCloudObject(IterableLikeCoder target, SdkComponents sdkComponents) {
        CloudObject base = CloudObject.forClass(clazz);
        return addComponents(
            base, Collections.<Coder<?>>singletonList(target.getElemCoder()), sdkComponents);
      }

      @Override
      public IterableLikeCoder<?, ?> fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> elemCoderList = getComponents(cloudObject);
        checkArgument(
            elemCoderList.size() == 1,
            "Expected 1 component for %s, got %s",
            cloudObject.getClassName(),
            elemCoderList.size());
        return InstanceBuilder.ofType(clazz)
            .fromFactoryMethod("of")
            .withArg(Coder.class, elemCoderList.get(0))
            .build();
      }

      @Override
      public Class<? extends IterableLikeCoder> getSupportedClass() {
        return clazz;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObject.forClass(clazz).getClassName();
      }
    };
  }

  public static CloudObjectTranslator<MapCoder> map() {
    return new CloudObjectTranslator<MapCoder>() {
      @Override
      public CloudObject toCloudObject(MapCoder target, SdkComponents sdkComponents) {
        CloudObject base = CloudObject.forClass(MapCoder.class);
        return addComponents(
            base,
            ImmutableList.<Coder<?>>of(target.getKeyCoder(), target.getValueCoder()),
            sdkComponents);
      }

      @Override
      public MapCoder<?, ?> fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> components = getComponents(cloudObject);
        checkArgument(
            components.size() == 2,
            "Expected 2 components for %s, got %s",
            MapCoder.class.getSimpleName(),
            components.size());
        return MapCoder.of(components.get(0), components.get(1));
      }

      @Override
      public Class<? extends MapCoder> getSupportedClass() {
        return MapCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObject.forClass(MapCoder.class).getClassName();
      }
    };
  }

  public static CloudObjectTranslator<TimestampedValue.TimestampedValueCoder> timestampedValue() {
    return new CloudObjectTranslator<TimestampedValue.TimestampedValueCoder>() {
      @Override
      public CloudObject toCloudObject(
          TimestampedValue.TimestampedValueCoder target, SdkComponents sdkComponents) {
        CloudObject base = CloudObject.forClass(TimestampedValue.TimestampedValueCoder.class);
        return addComponents(
            base, ImmutableList.<Coder<?>>of(target.getValueCoder()), sdkComponents);
      }

      @Override
      public TimestampedValue.TimestampedValueCoder<?> fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> components = getComponents(cloudObject);
        checkArgument(
            components.size() == 1,
            "Expected 1 components for %s, got %s",
            TimestampedValue.TimestampedValueCoder.class.getSimpleName(),
            components.size());
        return TimestampedValue.TimestampedValueCoder.of(components.get(0));
      }

      @Override
      public Class<? extends TimestampedValue.TimestampedValueCoder> getSupportedClass() {
        return TimestampedValue.TimestampedValueCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObject.forClass(TimestampedValue.TimestampedValueCoder.class).getClassName();
      }
    };
  }

  public static CloudObjectTranslator<NullableCoder> nullable() {
    return new CloudObjectTranslator<NullableCoder>() {
      @Override
      public CloudObject toCloudObject(NullableCoder target, SdkComponents sdkComponents) {
        CloudObject base = CloudObject.forClass(NullableCoder.class);
        return addComponents(
            base, Collections.<Coder<?>>singletonList(target.getValueCoder()), sdkComponents);
      }

      @Override
      public NullableCoder<?> fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> componentList = getComponents(cloudObject);
        checkArgument(
            componentList.size() == 1,
            "Expected 1 component for %s, got %s",
            NullableCoder.class.getSimpleName(),
            componentList.size());
        return NullableCoder.of(componentList.get(0));
      }

      @Override
      public Class<? extends NullableCoder> getSupportedClass() {
        return NullableCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObject.forClass(NullableCoder.class).getClassName();
      }
    };
  }

  public static CloudObjectTranslator<UnionCoder> union() {
    return new CloudObjectTranslator<UnionCoder>() {
      @Override
      public CloudObject toCloudObject(UnionCoder target, SdkComponents sdkComponents) {
        return addComponents(
            CloudObject.forClass(UnionCoder.class), target.getElementCoders(), sdkComponents);
      }

      @Override
      public UnionCoder fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> elementCoders = getComponents(cloudObject);
        return UnionCoder.of(elementCoders);
      }

      @Override
      public Class<? extends UnionCoder> getSupportedClass() {
        return UnionCoder.class;
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObject.forClass(UnionCoder.class).getClassName();
      }
    };
  }

  public static CloudObjectTranslator<CoGbkResultCoder> coGroupByKeyResult() {
    return new CloudObjectTranslator<CoGbkResultCoder>() {
      @Override
      public CloudObject toCloudObject(CoGbkResultCoder target, SdkComponents sdkComponents) {
        CloudObject base = CloudObject.forClass(CoGbkResultCoder.class);
        Structs.addObject(
            base, PropertyNames.CO_GBK_RESULT_SCHEMA, toCloudObject(target.getSchema()));
        return addComponents(
            base, Collections.singletonList(target.getUnionCoder()), sdkComponents);
      }

      private CloudObject toCloudObject(CoGbkResultSchema schema) {
        CloudObject result = CloudObject.forClass(CoGbkResultSchema.class);
        List<CloudObject> tags = new ArrayList<>(schema.getTupleTagList().size());
        for (TupleTag<?> tag : schema.getTupleTagList().getAll()) {
          CloudObject tagCloudObject = CloudObject.forClass(TupleTag.class);
          Structs.addString(tagCloudObject, PropertyNames.VALUE, tag.getId());
          tags.add(tagCloudObject);
        }
        Structs.addList(result, PropertyNames.TUPLE_TAGS, tags);
        return result;
      }

      @Override
      public CoGbkResultCoder fromCloudObject(CloudObject cloudObject) {
        List<Coder<?>> components = getComponents(cloudObject);
        checkArgument(
            components.size() == 1,
            "Expected 1 component for %s, got %s",
            CoGbkResultCoder.class.getSimpleName(),
            components.size());
        checkArgument(
            components.get(0) instanceof UnionCoder,
            "Expected only component to be a %s, got %s",
            UnionCoder.class.getSimpleName(),
            components.get(0).getClass().getName());
        return CoGbkResultCoder.of(
            schemaFromCloudObject(
                CloudObject.fromSpec(
                    Structs.getObject(cloudObject, PropertyNames.CO_GBK_RESULT_SCHEMA))),
            (UnionCoder) components.get(0));
      }

      @Override
      public Class<? extends CoGbkResultCoder> getSupportedClass() {
        return CoGbkResultCoder.class;
      }

      private CoGbkResultSchema schemaFromCloudObject(CloudObject cloudObject) {
        List<TupleTag<?>> tags = new ArrayList<>();
        List<Map<String, Object>> serializedTags =
            Structs.getListOfMaps(cloudObject, PropertyNames.TUPLE_TAGS, Collections.emptyList());
        for (Map<String, Object> serializedTag : serializedTags) {
          TupleTag<?> tag = new TupleTag<>(Structs.getString(serializedTag, PropertyNames.VALUE));
          tags.add(tag);
        }
        return CoGbkResultSchema.of(tags);
      }

      @Override
      public String cloudObjectClassName() {
        return CloudObject.forClass(CoGbkResultCoder.class).getClassName();
      }
    };
  }
}
