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
package org.apache.beam.runners.core.construction.resources;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.classgraph.ClassGraph;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.InstanceBuilder;

/** Pipeline options dedicated to detecting classpath resources. */
public interface PipelineResourcesOptions extends PipelineOptions {

  /**
   * The class of the pipeline resources detector factory that should be created and used to create
   * the detector. If not set explicitly, a default class will be used to instantiate the factory.
   */
  @JsonIgnore
  @Description(
      "The class of the pipeline resources detector factory that should be created and used to create "
          + "the detector. If not set explicitly, a default class will be used to instantiate the factory.")
  @Default.Class(ClasspathScanningResourcesDetectorFactory.class)
  Class<? extends PipelineResourcesDetector.Factory> getPipelineResourcesDetectorFactoryClass();

  void setPipelineResourcesDetectorFactoryClass(
      Class<? extends PipelineResourcesDetector.Factory> factoryClass);

  /**
   * Instance of a pipeline resources detection algorithm. If not set explicitly, a default
   * implementation will be used.
   */
  @JsonIgnore
  @Description(
      "Instance of a pipeline resources detection algorithm. If not set explicitly, a default implementation will be used")
  @Default.InstanceFactory(PipelineResourcesDetectorFactory.class)
  PipelineResourcesDetector getPipelineResourcesDetector();

  void setPipelineResourcesDetector(PipelineResourcesDetector pipelineResourcesDetector);

  /**
   * Creates {@link PipelineResourcesDetector} instance based on provided pipeline options or
   * default values set for them.
   */
  class PipelineResourcesDetectorFactory implements DefaultValueFactory<PipelineResourcesDetector> {

    @Override
    public PipelineResourcesDetector create(PipelineOptions options) {
      PipelineResourcesOptions resourcesOptions = options.as(PipelineResourcesOptions.class);

      PipelineResourcesDetector.Factory resourcesToStage =
          InstanceBuilder.ofType(PipelineResourcesDetector.Factory.class)
              .fromClass(resourcesOptions.getPipelineResourcesDetectorFactoryClass())
              .fromFactoryMethod("create")
              .build();

      return resourcesToStage.getPipelineResourcesDetector();
    }
  }

  /** Constructs the default {@link PipelineResourcesDetector} instance. */
  class ClasspathScanningResourcesDetectorFactory implements PipelineResourcesDetector.Factory {

    public static ClasspathScanningResourcesDetectorFactory create() {
      return new ClasspathScanningResourcesDetectorFactory();
    }

    @Override
    public PipelineResourcesDetector getPipelineResourcesDetector() {
      return new ClasspathScanningResourcesDetector(new ClassGraph());
    }
  }
}
