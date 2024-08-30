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
package org.apache.beam.runners.dataflow.worker.util;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjectTranslator;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.CoderCloudObjectTranslatorRegistrar;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.util.Structs;
import org.apache.beam.runners.dataflow.worker.WindmillKeyedWorkItem.FakeKeyedWorkItemCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Empty class which exists because the back end will sometimes insert uses of {@code
 * com.google.cloud.dataflow.sdk.util.TimerOrElement$TimerOrElementCoder} and we'd like to be able
 * to rename/move that without breaking things.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TimerOrElement {

  // TimerOrElement should never be created.
  private TimerOrElement() {}

  /**
   * Empty class which exists because the back end will sometimes insert uses of {@code
   * com.google.cloud.dataflow.dataflow.sdk.util.TimerOrElement$TimerOrElementCoder} and we'd like
   * to be able to rename/move that without breaking things.
   */
  public static class TimerOrElementCoder<ElemT> extends FakeKeyedWorkItemCoder<Object, ElemT> {

    private TimerOrElementCoder(Coder<ElemT> elemCoder) {
      super(elemCoder);
    }

    public static <T> TimerOrElementCoder<T> of(Coder<T> elemCoder) {
      return new TimerOrElementCoder<>(elemCoder);
    }

    @JsonCreator
    public static TimerOrElementCoder<?> of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS) List<Coder<?>> components) {
      return of(components.get(0));
    }
  }

  private static class TimerOrElementCloudObjectTranslator
      implements CloudObjectTranslator<TimerOrElementCoder> {
    @Override
    public CloudObject toCloudObject(TimerOrElementCoder target, SdkComponents sdkComponents) {
      throw new IllegalArgumentException("Should never be called");
    }

    @Override
    public TimerOrElementCoder fromCloudObject(CloudObject cloudObject) {
      List<Map<String, Object>> encodedComponents =
          Structs.getListOfMaps(
              cloudObject, PropertyNames.COMPONENT_ENCODINGS, Collections.emptyList());
      checkArgument(
          encodedComponents.size() == 1,
          "Expected 1 component for %s, got %s",
          TimerOrElementCoder.class.getSimpleName(),
          encodedComponents.size());
      CloudObject component = CloudObject.fromSpec(encodedComponents.get(0));
      return TimerOrElementCoder.of(CloudObjects.coderFromCloudObject(component));
    }

    @Override
    public Class<? extends TimerOrElementCoder> getSupportedClass() {
      return TimerOrElementCoder.class;
    }

    @Override
    public String cloudObjectClassName() {
      return "com.google.cloud.dataflow.sdk.util.TimerOrElement$TimerOrElementCoder";
    }
  }

  /** The registrar for {@link TimerOrElementCoder}. */
  @SuppressWarnings("unused")
  @AutoService(CoderCloudObjectTranslatorRegistrar.class)
  public static class TimerOrElementCloudObjectTranslatorRegistrar
      implements CoderCloudObjectTranslatorRegistrar {
    private static final TimerOrElementCloudObjectTranslator TRANSLATOR =
        new TimerOrElementCloudObjectTranslator();

    @Override
    public Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
        classesToTranslators() {
      return ImmutableMap.of();
    }

    @Override
    public Map<String, CloudObjectTranslator<? extends Coder>> classNamesToTranslators() {
      return Collections.singletonMap(TRANSLATOR.cloudObjectClassName(), TRANSLATOR);
    }
  }
}
