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

import com.google.api.services.dataflow.Dataflow;
import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.util.DataflowPathValidator;
import com.google.cloud.dataflow.sdk.util.GcsStager;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.util.PathValidator;
import com.google.cloud.dataflow.sdk.util.Stager;
import com.google.cloud.dataflow.sdk.util.Transport;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * Internal. Options used to control execution of the Dataflow SDK for
 * debugging and testing purposes.
 */
@Description("[Internal] Options used to control execution of the Dataflow SDK for "
    + "debugging and testing purposes.")
@Hidden
public interface DataflowPipelineDebugOptions extends PipelineOptions {

  /**
   * The default endpoint  to use with the Dataflow API.
   */
  static final String DEFAULT_API_ROOT = "https://dataflow.googleapis.com/";

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
  @Description("The URL for the Dataflow API. If the string contains \"://\""
      + " will be treated as the entire URL, otherwise will be treated relative to apiRootUrl.")
  @Default.String("v1b3/projects/")
  String getDataflowEndpoint();
  void setDataflowEndpoint(String value);

  /**
   * The list of backend experiments to enable.
   *
   * <p> Dataflow provides a number of experimental features that can be enabled
   * with this flag.
   *
   * <p> Please sync with the Dataflow team before enabling any experiments.
   */
  @Description("[Experimental] Dataflow provides a number of experimental features that can "
      + "be enabled with this flag. Please sync with the Dataflow team before enabling any "
      + "experiments.")
  List<String> getExperiments();
  void setExperiments(List<String> value);

  /**
   * The endpoint to use with the Dataflow API. dataflowEndpoint can override this value
   * if it contains an absolute URL, otherwise apiRootUrl will be combined with dataflowEndpoint
   * to generate the full URL to communicate with the Dataflow API.
   */
  @Description("The endpoint to use with the Dataflow API. dataflowEndpoint can override this "
      + "value if it contains an absolute URL, otherwise apiRootUrl will be combined with "
      + "dataflowEndpoint to generate the full URL to communicate with the Dataflow API.")
  @Default.String(DEFAULT_API_ROOT)
  String getApiRootUrl();
  void setApiRootUrl(String value);

  /**
   * The path to write the translated Dataflow job specification out to
   * at job submission time. The Dataflow job specification will be represented in JSON
   * format.
   */
  @Description("The path to write the translated Dataflow job specification out to "
      + "at job submission time. The Dataflow job specification will be represented in JSON "
      + "format.")
  String getDataflowJobFile();
  void setDataflowJobFile(String value);

  /**
   * The class of the validator that should be created and used to validate paths.
   * If pathValidator has not been set explicitly, an instance of this class will be
   * constructed and used as the path validator.
   */
  @Description("The class of the validator that should be created and used to validate paths. "
      + "If pathValidator has not been set explicitly, an instance of this class will be "
      + "constructed and used as the path validator.")
  @Default.Class(DataflowPathValidator.class)
  Class<? extends PathValidator> getPathValidatorClass();
  void setPathValidatorClass(Class<? extends PathValidator> validatorClass);

  /**
   * The path validator instance that should be created and used to validate paths.
   * If no path validator has been set explicitly, the default is to use the instance factory that
   * constructs a path validator based upon the currently set pathValidatorClass.
   */
  @JsonIgnore
  @Description("The path validator instance that should be created and used to validate paths. "
      + "If no path validator has been set explicitly, the default is to use the instance factory "
      + "that constructs a path validator based upon the currently set pathValidatorClass.")
  @Default.InstanceFactory(PathValidatorFactory.class)
  PathValidator getPathValidator();
  void setPathValidator(PathValidator validator);

  /**
   * The class responsible for staging resources to be accessible by workers
   * during job execution.
   */
  @Description("The class of the stager that should be created and used to stage resources. "
      + "If stager has not been set explicitly, an instance of this class will be "
      + "constructed and used as the resource stager.")
  @Default.Class(GcsStager.class)
  Class<? extends Stager> getStagerClass();
  void setStagerClass(Class<? extends Stager> stagerClass);

  /**
   * The resource stager instance that should be created and used to stage resources.
   * If no stager has been set explicitly, the default is to use the instance factory
   * that constructs a resource stager based upon the currently set stagerClass.
   */
  @JsonIgnore
  @Description("The resource stager instance that should be created and used to stage resources. "
      + "If no stager has been set explicitly, the default is to use the instance factory "
      + "that constructs a resource stager based upon the currently set stagerClass.")
  @Default.InstanceFactory(StagerFactory.class)
  Stager getStager();
  void setStager(Stager stager);

  /**
   * An instance of the Dataflow client. Defaults to creating a Dataflow client
   * using the current set of options.
   */
  @JsonIgnore
  @Description("An instance of the Dataflow client. Defaults to creating a Dataflow client "
      + "using the current set of options.")
  @Default.InstanceFactory(DataflowClientFactory.class)
  Dataflow getDataflowClient();
  void setDataflowClient(Dataflow value);

  /** Returns the default Dataflow client built from the passed in PipelineOptions. */
  public static class DataflowClientFactory implements DefaultValueFactory<Dataflow> {
    @Override
    public Dataflow create(PipelineOptions options) {
        return Transport.newDataflowClient(options.as(DataflowPipelineOptions.class)).build();
    }
  }

  /**
   * Whether to reload the currently running pipeline with the same name as this one.
   */
  @JsonIgnore
  @Description("If set, replace the existing pipeline with the name specified by --jobName with "
      + "this pipeline, preserving state.")
  @Experimental
  boolean getReload();
  void setReload(boolean value);

  /**
   * Root URL for use with the Pubsub API.
   */
  @Description("Root URL for use with the Pubsub API")
  @Default.String("https://pubsub.googleapis.com")
  String getPubsubRootUrl();
  void setPubsubRootUrl(String value);

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
