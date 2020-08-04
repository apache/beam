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
package org.apache.beam.runners.dataflow.options;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.api.services.dataflow.Dataflow;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.DataflowTransport;
import org.apache.beam.runners.dataflow.util.GcsStager;
import org.apache.beam.runners.dataflow.util.Stager;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.InstanceBuilder;

/**
 * Internal. Options used to control execution of the Dataflow SDK for debugging and testing
 * purposes.
 */
@Description(
    "[Internal] Options used to control execution of the Dataflow SDK for "
        + "debugging and testing purposes.")
@Hidden
public interface DataflowPipelineDebugOptions extends ExperimentalOptions, PipelineOptions {

  /**
   * The root URL for the Dataflow API. {@code dataflowEndpoint} can override this value if it
   * contains an absolute URL, otherwise {@code apiRootUrl} will be combined with {@code
   * dataflowEndpoint} to generate the full URL to communicate with the Dataflow API.
   */
  @Description(
      "The root URL for the Dataflow API. dataflowEndpoint can override this "
          + "value if it contains an absolute URL, otherwise apiRootUrl will be combined with "
          + "dataflowEndpoint to generate the full URL to communicate with the Dataflow API.")
  @Default.String(Dataflow.DEFAULT_ROOT_URL)
  String getApiRootUrl();

  void setApiRootUrl(String value);

  /**
   * Dataflow endpoint to use.
   *
   * <p>Defaults to the current version of the Google Cloud Dataflow API, at the time the current
   * SDK version was released.
   *
   * <p>If the string contains "://", then this is treated as a URL, otherwise {@link
   * #getApiRootUrl()} is used as the root URL.
   */
  @Description(
      "The URL for the Dataflow API. If the string contains \"://\", this"
          + " will be treated as the entire URL, otherwise will be treated relative to apiRootUrl.")
  @Default.String(Dataflow.DEFAULT_SERVICE_PATH)
  String getDataflowEndpoint();

  void setDataflowEndpoint(String value);

  /**
   * The path to write the translated Dataflow job specification out to at job submission time. The
   * Dataflow job specification will be represented in JSON format.
   */
  @Description(
      "The path to write the translated Dataflow job specification out to "
          + "at job submission time. The Dataflow job specification will be represented in JSON "
          + "format.")
  String getDataflowJobFile();

  void setDataflowJobFile(String value);

  /**
   * The class responsible for staging resources to be accessible by workers during job execution.
   * If stager has not been set explicitly, an instance of this class will be created and used as
   * the resource stager.
   */
  @Description(
      "The class of the stager that should be created and used to stage resources. "
          + "If stager has not been set explicitly, an instance of the this class will be created "
          + "and used as the resource stager.")
  @Default.Class(GcsStager.class)
  Class<? extends Stager> getStagerClass();

  void setStagerClass(Class<? extends Stager> stagerClass);

  /**
   * The resource stager instance that should be used to stage resources. If no stager has been set
   * explicitly, the default is to use the instance factory that constructs a resource stager based
   * upon the currently set stagerClass.
   */
  @JsonIgnore
  @Description(
      "The resource stager instance that should be used to stage resources. "
          + "If no stager has been set explicitly, the default is to use the instance factory "
          + "that constructs a resource stager based upon the currently set stagerClass.")
  @Default.InstanceFactory(StagerFactory.class)
  Stager getStager();

  void setStager(Stager stager);

  /**
   * An instance of the Dataflow client. Defaults to creating a Dataflow client using the current
   * set of options.
   */
  @JsonIgnore
  @Description(
      "An instance of the Dataflow client. Defaults to creating a Dataflow client "
          + "using the current set of options.")
  @Default.InstanceFactory(DataflowClientFactory.class)
  Dataflow getDataflowClient();

  void setDataflowClient(Dataflow value);

  /** Returns the default Dataflow client built from the passed in PipelineOptions. */
  class DataflowClientFactory implements DefaultValueFactory<Dataflow> {
    @Override
    public Dataflow create(PipelineOptions options) {
      return DataflowTransport.newDataflowClient(options.as(DataflowPipelineOptions.class)).build();
    }
  }

  /**
   * Mapping of old PTranform names to new ones, specified as JSON <code>{"oldName":"newName",...}
   * </code>. To mark a transform as deleted, make newName the empty string.
   */
  @JsonIgnore
  @Description(
      "Mapping of old PTranform names to new ones, specified as JSON "
          + "{\"oldName\":\"newName\",...}. To mark a transform as deleted, make newName the empty "
          + "string.")
  Map<String, String> getTransformNameMapping();

  void setTransformNameMapping(Map<String, String> value);

  /** Custom windmill_main binary to use with the streaming runner. */
  @Description("Custom windmill_main binary to use with the streaming runner")
  String getOverrideWindmillBinary();

  void setOverrideWindmillBinary(String value);

  /** Custom windmill service endpoint. */
  @Description("Custom windmill service endpoint.")
  String getWindmillServiceEndpoint();

  void setWindmillServiceEndpoint(String value);

  @Description("Port for communicating with a remote windmill service.")
  @Default.Integer(443)
  int getWindmillServicePort();

  void setWindmillServicePort(int value);

  /**
   * Number of threads to use on the Dataflow worker harness. If left unspecified, the Dataflow
   * service will compute an appropriate number of threads to use.
   */
  @Description(
      "Number of threads to use on the Dataflow worker harness. If left unspecified, "
          + "the Dataflow service will compute an appropriate number of threads to use.")
  int getNumberOfWorkerHarnessThreads();

  void setNumberOfWorkerHarnessThreads(int value);

  /**
   * If {@literal true}, save a heap dump before killing a thread or process which is GC thrashing
   * or out of memory. The location of the heap file will either be echoed back to the user, or the
   * user will be given the opportunity to download the heap file.
   *
   * <p>CAUTION: Heap dumps can of comparable size to the default boot disk. Consider increasing the
   * boot disk size before setting this flag to true.
   */
  @Description(
      "If {@literal true}, save a heap dump before killing a thread or process "
          + "which is GC thrashing or out of memory.")
  boolean getDumpHeapOnOOM();

  void setDumpHeapOnOOM(boolean dumpHeapBeforeExit);

  /**
   * The GC thrashing threshold percentage. A given period of time is considered "thrashing" if this
   * percentage of CPU time is spent in garbage collection. Dataflow will force fail tasks after
   * sustained periods of thrashing.
   *
   * <p>If {@literal 100} is given as the value, MemoryMonitor will be disabled.
   */
  @Description(
      "The GC thrashing threshold percentage. A given period of time is considered \"thrashing\" if this "
          + "percentage of CPU time is spent in garbage collection. Dataflow will force fail tasks after "
          + "sustained periods of thrashing.")
  @Default.Double(50.0)
  Double getGCThrashingPercentagePerPeriod();

  void setGCThrashingPercentagePerPeriod(Double value);

  /**
   * The size of the worker's in-memory cache, in megabytes.
   *
   * <p>Currently, this cache is used for storing read values of side inputs. as well as the state
   * for streaming jobs.
   */
  @Description("The size of the worker's in-memory cache, in megabytes.")
  @Default.Integer(100)
  Integer getWorkerCacheMb();

  void setWorkerCacheMb(Integer value);

  /**
   * CAUTION: This option implies dumpHeapOnOOM, and has similar caveats. Specifically, heap dumps
   * can of comparable size to the default boot disk. Consider increasing the boot disk size before
   * setting this flag to true.
   */
  @Description(
      "[EXPERIMENTAL] Set to a GCS bucket (directory) to upload heap dumps to the given location.\n"
          + "Enabling this implies that heap dumps should be generated on OOM (--dumpHeapOnOOM=true)\n"
          + "Uploads will continue until the pipeline is stopped or updated without this option.\n")
  @Experimental
  String getSaveHeapDumpsToGcsPath();

  void setSaveHeapDumpsToGcsPath(String gcsPath);

  /** Creates a {@link Stager} object using the class specified in {@link #getStagerClass()}. */
  class StagerFactory implements DefaultValueFactory<Stager> {
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
