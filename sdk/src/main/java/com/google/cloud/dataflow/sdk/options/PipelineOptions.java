/*
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
 */

package com.google.cloud.dataflow.sdk.options;

import com.google.cloud.dataflow.sdk.options.ProxyInvocationHandler.Deserializer;
import com.google.cloud.dataflow.sdk.options.ProxyInvocationHandler.Serializer;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Dataflow SDK pipeline configuration options.
 * <p>
 * Serialization
 * <p>
 * For runners that execute their work remotely, every property available within PipelineOptions
 * must either be serializable using Jackson's {@link ObjectMapper} or the getter method for the
 * property annotated with {@link JsonIgnore @JsonIgnore}.
 * <p>
 * It is an error to have the same property available in multiple interfaces with only some
 * of them being annotated with {@link JsonIgnore @JsonIgnore}. It is also an error to mark a
 * setter for a property with {@link JsonIgnore @JsonIgnore}.
 */
@JsonSerialize(using = Serializer.class)
@JsonDeserialize(using = Deserializer.class)
@ThreadSafe
public interface PipelineOptions {
  /**
   * Transforms this object into an object of type {@code <T>}. {@code <T>} must extend
   * {@link PipelineOptions}.
   * <p>
   * If {@code <T>} is not registered with the {@link PipelineOptionsFactory}, then we
   * attempt to verify that {@code <T>} is composable with every interface that this
   * instance of the {@code PipelineOptions} has seen.
   *
   * @param kls The class of the type to transform to.
   * @return An object of type kls.
   */
  <T extends PipelineOptions> T as(Class<T> kls);

  /**
   * Makes a deep clone of this object, and transforms the cloned object into the specified
   * type {@code kls}. See {@link #as} for more information about the conversion.
   * <p>
   * Properties that are marked with {@code @JsonIgnore} will not be cloned.
   */
  <T extends PipelineOptions> T cloneAs(Class<T> kls);

  @Validation.Required
  @Description("The pipeline runner that will be used to execute the pipeline. "
      + "For registered runners, the class name can be specified, otherwise the fully"
      + "qualified name needs to be specified.")
  @Default.Class(DirectPipelineRunner.class)
  Class<? extends PipelineRunner<?>> getRunner();
  void setRunner(Class<? extends PipelineRunner<?>> kls);
}
