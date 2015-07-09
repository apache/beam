/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.util.Serializer;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import java.util.HashMap;
import java.util.Map;

/**
 * Constructs a Sink from a Dataflow service protocol Sink definition.
 *
 * <p> A SinkFactory concrete "subclass" should define a method with the
 * following signature:
 * <pre> {@code
 * static SomeSinkSubclass<T> create(PipelineOptions, CloudObject,
 *                                   Coder<T>, ExecutionContext,
 *                                   CounterSet.AddCounterMutator);
 * } </pre>
 */
public final class SinkFactory {
  // Do not instantiate.
  private SinkFactory() {}

  /**
   * A map from the short names of predefined sinks to their full
   * factory class names.
   */
  static Map<String, String> predefinedSinkFactories = new HashMap<>();

  static {
    predefinedSinkFactories.put("TextSink",
                                TextSinkFactory.class.getName());
    predefinedSinkFactories.put("AvroSink",
                                AvroSinkFactory.class.getName());
    predefinedSinkFactories.put("ShuffleSink",
                                ShuffleSinkFactory.class.getName());
    predefinedSinkFactories.put("PubsubSink",
                                PubsubSink.class.getName());
    predefinedSinkFactories.put("WindmillSink",
                                WindmillSink.class.getName());
  }

  /**
   * Creates a {@link Sink} from a Dataflow API Sink definition.
   *
   * @throws Exception if the sink could not be decoded and
   * constructed
   */
  @SuppressWarnings("serial")
  public static <T> Sink<T> create(
      PipelineOptions options,
      com.google.api.services.dataflow.model.Sink cloudSink,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator)
      throws Exception {
    Coder<T> coder = Serializer.deserialize(cloudSink.getCodec(), Coder.class);
    CloudObject object = CloudObject.fromSpec(cloudSink.getSpec());

    String className = predefinedSinkFactories.get(object.getClassName());
    if (className == null) {
      className = object.getClassName();
    }

    try {
      return InstanceBuilder.ofType(new TypeDescriptor<Sink<T>>() {})
          .fromClassName(className)
          .fromFactoryMethod("create")
          .withArg(PipelineOptions.class, options)
          .withArg(CloudObject.class, object)
          .withArg(Coder.class, coder)
          .withArg(ExecutionContext.class, executionContext)
          .withArg(CounterSet.AddCounterMutator.class, addCounterMutator)
          .build();

    } catch (ClassNotFoundException exn) {
      throw new Exception(
          "unable to create a sink from " + cloudSink, exn);
    }
  }
}
