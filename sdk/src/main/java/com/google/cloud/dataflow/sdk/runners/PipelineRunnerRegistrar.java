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

package com.google.cloud.dataflow.sdk.runners;

import com.google.auto.service.AutoService;
import java.util.ServiceLoader;

/**
 * {@link PipelineRunner} creators have the ability to automatically have their
 * {@link PipelineRunner} registered with this SDK by creating a {@link ServiceLoader} entry
 * and a concrete implementation of this interface.
 *
 * <p>Note that automatic registration of any
 * {@link com.google.cloud.dataflow.sdk.options.PipelineOptions} requires users
 * conform to the limit that each {@link PipelineRunner}'s
 * {@link Class#getSimpleName() simple name} must be unique.
 *
 * <p>It is optional but recommended to use one of the many build time tools such as
 * {@link AutoService} to generate the necessary META-INF files automatically.
 */
public interface PipelineRunnerRegistrar {
  /**
   * Get the set of {@link PipelineRunner PipelineRunners} to register.
   */
  public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners();
}
