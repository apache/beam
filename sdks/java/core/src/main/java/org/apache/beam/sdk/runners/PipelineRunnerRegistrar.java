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
package org.apache.beam.sdk.runners;

import com.google.auto.service.AutoService;
import java.util.ServiceLoader;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Internal;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>{@link PipelineRunner} creators have the ability to automatically have their {@link
 * PipelineRunner} registered with this SDK by creating a {@link ServiceLoader} entry and a concrete
 * implementation of this interface.
 *
 * <p>Note that automatic registration of any {@link org.apache.beam.sdk.options.PipelineOptions}
 * requires users conform to the limit that each {@link PipelineRunner}'s {@link
 * Class#getSimpleName() simple name} must be unique.
 *
 * <p>It is optional but recommended to use one of the many build time tools such as {@link
 * AutoService} to generate the necessary META-INF files automatically.
 */
@Internal
public interface PipelineRunnerRegistrar {
  /** Get the set of {@link PipelineRunner PipelineRunners} to register. */
  Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners();
}
