/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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
import com.google.cloud.dataflow.sdk.util.CloudSourceUtils;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.util.Serializer;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;
import com.google.common.reflect.TypeToken;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Constructs a Source from a Dataflow API Source definition.
 *
 * A SourceFactory concrete "subclass" should define a method with the following
 * signature:
 * <pre> {@code
 * static SomeSourceSubclass<T> create(PipelineOptions, CloudObject,
 *                                     Coder<T>, ExecutionContext);
 * } </pre>
 */
public final class SourceFactory {
  // Do not instantiate.
  private SourceFactory() {}

  /**
   * A map from the short names of predefined sources to
   * their full factory class names.
   */
  static Map<String, String> predefinedSourceFactories = new HashMap<>();

  static {
    predefinedSourceFactories.put(
        "TextSource",
        TextSourceFactory.class.getName());
    predefinedSourceFactories.put(
        "AvroSource",
        AvroSourceFactory.class.getName());
    predefinedSourceFactories.put(
        "UngroupedShuffleSource",
        UngroupedShuffleSourceFactory.class.getName());
    predefinedSourceFactories.put(
        "PartitioningShuffleSource",
        PartitioningShuffleSourceFactory.class.getName());
    predefinedSourceFactories.put(
        "GroupingShuffleSource",
        GroupingShuffleSourceFactory.class.getName());
    predefinedSourceFactories.put(
        "InMemorySource",
        InMemorySourceFactory.class.getName());
    predefinedSourceFactories.put(
        "BigQuerySource",
        BigQuerySourceFactory.class.getName());
  }

  /**
   * Creates a Source from a Dataflow API Source definition.
   *
   * @throws Exception if the source could not be decoded and
   * constructed
   */
  public static <T> Source<T> create(
      @Nullable PipelineOptions options,
      com.google.api.services.dataflow.model.Source cloudSource,
      @Nullable ExecutionContext executionContext)
      throws Exception {
    cloudSource = CloudSourceUtils.flattenBaseSpecs(cloudSource);
    Coder<T> coder = Serializer.deserialize(cloudSource.getCodec(), Coder.class);
    CloudObject object = CloudObject.fromSpec(cloudSource.getSpec());

    String sourceFactoryClassName = predefinedSourceFactories.get(object.getClassName());
    if (sourceFactoryClassName == null) {
      sourceFactoryClassName = object.getClassName();
    }

    try {
      return InstanceBuilder.ofType(new TypeToken<Source<T>>() {})
          .fromClassName(sourceFactoryClassName)
          .fromFactoryMethod("create")
          .withArg(PipelineOptions.class, options)
          .withArg(CloudObject.class, object)
          .withArg(Coder.class, coder)
          .withArg(ExecutionContext.class, executionContext)
          .build();

    } catch (ClassNotFoundException exn) {
      throw new Exception(
          "unable to create a source from " + cloudSource, exn);
    }
  }
}
