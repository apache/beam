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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.Structs;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;

/** Utilities for creating {@link CloudObjectTranslator} instances for {@link Coder Coders}. */
class CloudObjectTranslators {
  private CloudObjectTranslators() {}

  private static CloudObject addComponents(CloudObject base, List<? extends Coder<?>> components) {
    if (!components.isEmpty()) {
      List<CloudObject> cloudComponents = new ArrayList<>(components.size());
      for (Coder component : components) {
        cloudComponents.add(CloudObjects.asCloudObject(component));
      }
      Structs.addList(base, PropertyNames.COMPONENT_ENCODINGS, cloudComponents);
    }

    return base;
  }

  private static List<Coder<?>> getComponents(CloudObject target) {
    List<Map<String, Object>> cloudComponents =
        Structs.getListOfMaps(
            target,
            PropertyNames.COMPONENT_ENCODINGS,
            Collections.<Map<String, Object>>emptyList());
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
      public CloudObject toCloudObject(KvCoder target) {
        CloudObject result = CloudObject.forClassName(CloudObjectKinds.KIND_PAIR);
        Structs.addBoolean(result, PropertyNames.IS_PAIR_LIKE, true);
        return addComponents(
            result, ImmutableList.<Coder<?>>of(target.getKeyCoder(), target.getValueCoder()));
      }

      @Override
      public KvCoder fromCloudObject(CloudObject object) {
        return KvCoder.of(getComponents(object));
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
      public CloudObject toCloudObject(IterableCoder target) {
        CloudObject result = CloudObject.forClassName(CloudObjectKinds.KIND_STREAM);
        Structs.addBoolean(result, PropertyNames.IS_STREAM_LIKE, true);
        return addComponents(
            result, Collections.<Coder<?>>singletonList(target.getElemCoder()));
      }

      @Override
      public IterableCoder fromCloudObject(CloudObject object) {
        return IterableCoder.of(getComponents(object));
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
      public CloudObject toCloudObject(LengthPrefixCoder target) {
        return addComponents(
            CloudObject.forClassName(CloudObjectKinds.KIND_LENGTH_PREFIX),
            Collections.<Coder<?>>singletonList(target.getValueCoder()));
      }

      @Override
      public LengthPrefixCoder fromCloudObject(CloudObject object) {
        return LengthPrefixCoder.of(getComponents(object));
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
      public CloudObject toCloudObject(GlobalWindow.Coder target) {
        return addComponents(
            CloudObject.forClassName(CloudObjectKinds.KIND_GLOBAL_WINDOW),
            Collections.<Coder<?>>emptyList());
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
      public CloudObject toCloudObject(IntervalWindowCoder target) {
        return addComponents(
            CloudObject.forClassName(CloudObjectKinds.KIND_INTERVAL_WINDOW),
            Collections.<Coder<?>>emptyList());
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

  /**
   * Returns a {@link CloudObjectTranslator} that produces a {@link CloudObject} that is of kind
   * "windowed_value".
   */
  static CloudObjectTranslator<FullWindowedValueCoder> windowedValue() {
    return new CloudObjectTranslator<FullWindowedValueCoder>() {
      @Override
      public CloudObject toCloudObject(FullWindowedValueCoder target) {
        CloudObject result = CloudObject.forClassName(CloudObjectKinds.KIND_WINDOWED_VALUE);
        Structs.addBoolean(result, PropertyNames.IS_WRAPPER, true);
        return addComponents(
            result, ImmutableList.<Coder<?>>of(target.getValueCoder(), target.getWindowCoder()));
      }

      @Override
      public FullWindowedValueCoder fromCloudObject(CloudObject object) {
        return FullWindowedValueCoder.of(getComponents(object));
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
      public CloudObject toCloudObject(ByteArrayCoder target) {
        return addComponents(
            CloudObject.forClass(target.getClass()), Collections.<Coder<?>>emptyList());
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
        return CloudObject.forClass(ByteArrayCoder.class).getClassName();
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
      public CloudObject toCloudObject(VarLongCoder target) {
        return addComponents(
            CloudObject.forClass(target.getClass()), Collections.<Coder<?>>emptyList());
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
  public static CloudObjectTranslator<? extends CustomCoder> custom() {
    return new CloudObjectTranslator<CustomCoder>() {
      @Override
      public CloudObject toCloudObject(CustomCoder target) {
        CloudObject cloudObject = CloudObject.forClass(CustomCoder.class);
        Structs.addString(cloudObject, TYPE_FIELD, target.getClass().getName());
        Structs.addString(
            cloudObject,
            CODER_FIELD,
            StringUtils.byteArrayToJsonString(SerializableUtils.serializeToByteArray(target)));
        return cloudObject;
      }

      @Override
      public CustomCoder fromCloudObject(CloudObject cloudObject) {
        String serializedCoder = Structs.getString(cloudObject, CODER_FIELD);
        String type = Structs.getString(cloudObject, TYPE_FIELD);
        return (CustomCoder<?>)
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
      public CloudObject toCloudObject(T target) {
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
}
