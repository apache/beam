/*
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
 */

package com.google.cloud.dataflow.sdk.options;

import com.google.cloud.dataflow.sdk.util.DataflowPathValidator;
import com.google.cloud.dataflow.sdk.util.GcsStager;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.util.PathValidator;
import com.google.cloud.dataflow.sdk.util.Stager;

import java.util.List;

/**
 * Options used for testing and debugging the Dataflow SDK.
 */
public interface DataflowPipelineDebugOptions extends PipelineOptions {
  /**
   * Dataflow endpoint to use.
   *
   * <p> Defaults to the current version of the Google Cloud Dataflow
   * API, at the time the current SDK version was released.
   *
   * <p> If the string contains "://", then this is treated as a url,
   * otherwise {@link #getApiRootUrl()} is used as the root
   * url.
   */
  @Description("Cloud Dataflow Endpoint")
  @Default.String("dataflow/v1b3/projects/")
  String getDataflowEndpoint();
  void setDataflowEndpoint(String value);

  /**
   * The list of backend experiments to enable.
   *
   * <p> Dataflow provides a number of experimental features that can be enabled
   * with this flag.
   *
   * <p> Please sync with the Dataflow team when enabling any experiments.
   */
  @Description("Backend experiments to enable.")
  List<String> getExperiments();
  void setExperiments(List<String> value);

  /**
   * The API endpoint to use when communicating with the Dataflow service.
   */
  @Description("Google Cloud root API")
  @Default.String("https://www.googleapis.com/")
  String getApiRootUrl();
  void setApiRootUrl(String value);

  /**
   * The path to write the translated Dataflow specification out to
   * at job submission time.
   */
  @Description("File for writing dataflow job descriptions")
  String getDataflowJobFile();
  void setDataflowJobFile(String value);

  /**
   * The name of the validator class used to validate path names.
   */
  @Description("The validator class used to validate path names.")
  @Default.Class(DataflowPathValidator.class)
  Class<? extends PathValidator> getPathValidatorClass();
  void setPathValidatorClass(Class<? extends PathValidator> validatorClass);

  /**
   * The validator class used to validate path names.
   */
  @Description("The validator class used to validate path names.")
  @Default.InstanceFactory(PathValidatorFactory.class)
  PathValidator getPathValidator();
  void setPathValidator(PathValidator validator);

  /**
   * The class used to stage files.
   */
  @Description("The class used to stage files.")
  @Default.Class(GcsStager.class)
  Class<? extends Stager> getStagerClass();
  void setStagerClass(Class<? extends Stager> stagerClass);

  /**
   * The stager instance used to stage files.
   */
  @Description("The class use to stage packages.")
  @Default.InstanceFactory(StagerFactory.class)
  Stager getStager();
  void setStager(Stager stager);

  /**
   * Creates a {@link PathValidator} object using the class specified in
   * {@link #getPathValidatorClass()}.
   */
  public static class PathValidatorFactory implements DefaultValueFactory<PathValidator> {
      @Override
      public PathValidator create(PipelineOptions options) {
      DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
      return InstanceBuilder.ofType(PathValidator.class)
          .fromClass(debugOptions.getPathValidatorClass())
          .fromFactoryMethod("fromOptions")
          .withArg(PipelineOptions.class, options)
          .build();
    }
  }

  /**
   * Creates a {@link Stager} object using the class specified in
   * {@link #getStagerClass()}.
   */
  public static class StagerFactory implements DefaultValueFactory<Stager> {
      @Override
      public Stager create(PipelineOptions options) {
      DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
      return InstanceBuilder.ofType(Stager.class)
          .fromClass(debugOptions.getStagerClass())
          .fromFactoryMethod("fromOptions")
          .withArg(PipelineOptions.class, options)
          .build();
    }
  }
}
