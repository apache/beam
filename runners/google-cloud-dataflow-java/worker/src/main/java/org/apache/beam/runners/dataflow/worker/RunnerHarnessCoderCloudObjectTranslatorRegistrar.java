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
package org.apache.beam.runners.dataflow.worker;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjectTranslator;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.CoderCloudObjectTranslatorRegistrar;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.Structs;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * A registrar for {@link CloudObjectTranslator}s for the Dataflow runner harness.
 *
 * <p>See {@link CoderCloudObjectTranslatorRegistrar} for more details.
 */
@AutoService(CoderCloudObjectTranslatorRegistrar.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class RunnerHarnessCoderCloudObjectTranslatorRegistrar
    implements CoderCloudObjectTranslatorRegistrar {

  @Override
  public Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      classesToTranslators() {
    return ImmutableMap.of();
  }

  @Override
  public Map<String, CloudObjectTranslator<? extends Coder>> classNamesToTranslators() {
    return ImmutableMap.<String, CloudObjectTranslator<? extends Coder>>builder()
        .put("kind:ism_record", new IsmRecordCoderCloudObjectTranslator())
        .put("kind:fixed_big_endian_int32", atomic(BigEndianIntegerCoder.class))
        .put("kind:fixed_big_endian_int64", atomic(BigEndianLongCoder.class))
        .put("kind:var_int32", atomic(VarIntCoder.class))
        .put("kind:varint", atomic(VarLongCoder.class))
        .put("kind:void", atomic(VoidCoder.class))
        .build();
  }

  private static class IsmRecordCoderCloudObjectTranslator
      implements CloudObjectTranslator<IsmRecordCoder<?>> {

    @Override
    public CloudObject toCloudObject(IsmRecordCoder<?> target, SdkComponents sdkComponents) {
      throw new UnsupportedOperationException();
    }

    @Override
    public IsmRecordCoder<?> fromCloudObject(CloudObject cloudObject) {
      List<Coder<?>> coders = getComponents(cloudObject);
      return IsmRecordCoder.of(
          Structs.getLong(cloudObject, "num_shard_key_coders").intValue(),
          0,
          coders.subList(0, coders.size() - 1),
          coders.get(coders.size() - 1));
    }

    @Override
    public Class getSupportedClass() {
      return IsmRecordCoder.class;
    }

    @Override
    public String cloudObjectClassName() {
      return "kind:ism_record";
    }
  }

  private static <T extends Coder> CloudObjectTranslator<T> atomic(final Class<T> coderClass) {
    // Make sure that the instance will be instantiable from the class.
    InstanceBuilder.ofType(coderClass).fromFactoryMethod("of").build();
    return new CloudObjectTranslator<T>() {
      @Override
      public CloudObject toCloudObject(T target, SdkComponents sdkComponents) {
        throw new UnsupportedOperationException();
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
}
