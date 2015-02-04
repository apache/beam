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

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.CloudSourceUtils;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.util.Serializer;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.common.reflect.TypeToken;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Constructs a Reader from a Dataflow API Source definition.
 *
 * A ReaderFactory concrete "subclass" should define a method with the following
 * signature:
 * <pre> {@code
 * static SomeReaderSubclass<T> create(PipelineOptions, CloudObject,
 *                                     Coder<T>, ExecutionContext);
 * } </pre>
 */
public final class ReaderFactory {
  // Do not instantiate.
  private ReaderFactory() {}

  /**
   * A map from the short names of predefined sources to
   * their full factory class names.
   */
  static Map<String, String> predefinedReaderFactories = new HashMap<>();

  static {
    predefinedReaderFactories.put("TextSource", TextReaderFactory.class.getName());
    predefinedReaderFactories.put("AvroSource", AvroReaderFactory.class.getName());
    predefinedReaderFactories.put(
        "UngroupedShuffleSource", UngroupedShuffleReaderFactory.class.getName());
    predefinedReaderFactories.put(
        "PartitioningShuffleSource", PartitioningShuffleReaderFactory.class.getName());
    predefinedReaderFactories.put(
        "GroupingShuffleSource", GroupingShuffleReaderFactory.class.getName());
    predefinedReaderFactories.put("InMemorySource", InMemoryReaderFactory.class.getName());
    predefinedReaderFactories.put("BigQuerySource", BigQueryReaderFactory.class.getName());
    predefinedReaderFactories.put(
        "com.google.cloud.dataflow.sdk.runners.worker.BucketingWindmillSource",
        WindowingWindmillReader.class.getName());
    predefinedReaderFactories.put(
        "WindowingWindmillReader", WindowingWindmillReader.class.getName());
    predefinedReaderFactories.put(
        "com.google.cloud.dataflow.sdk.runners.worker.UngroupedWindmillSource",
        UngroupedWindmillReader.class.getName());
    predefinedReaderFactories.put(
        "UngroupedWindmillReader", UngroupedWindmillReader.class.getName());
    predefinedReaderFactories.put(
        "com.google.cloud.dataflow.sdk.runners.worker.PubsubSource",
        PubsubReader.class.getName());
    predefinedReaderFactories.put(
        "PubsubReader", PubsubReader.class.getName());
  }

  /**
   * Creates a Reader from a Dataflow API Source definition.
   *
   * @throws Exception if the source could not be decoded and
   * constructed
   */
  public static <T> Reader<T> create(@Nullable PipelineOptions options, Source cloudSource,
      @Nullable ExecutionContext executionContext) throws Exception {
    cloudSource = CloudSourceUtils.flattenBaseSpecs(cloudSource);
    Coder<T> coder = Serializer.deserialize(cloudSource.getCodec(), Coder.class);
    CloudObject object = CloudObject.fromSpec(cloudSource.getSpec());

    String sourceFactoryClassName = predefinedReaderFactories.get(object.getClassName());
    if (sourceFactoryClassName == null) {
      sourceFactoryClassName = object.getClassName();
    }

    try {
      return InstanceBuilder.ofType(new TypeToken<Reader<T>>() {})
          .fromClassName(sourceFactoryClassName)
          .fromFactoryMethod("create")
          .withArg(PipelineOptions.class, options)
          .withArg(CloudObject.class, object)
          .withArg(Coder.class, coder)
          .withArg(ExecutionContext.class, executionContext)
          .build();

    } catch (ClassNotFoundException exn) {
      throw new Exception("unable to create a source from " + cloudSource, exn);
    }
  }
}
