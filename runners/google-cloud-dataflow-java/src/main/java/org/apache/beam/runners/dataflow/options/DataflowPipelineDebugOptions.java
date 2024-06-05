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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.MemoryMonitorOptions;
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
public interface DataflowPipelineDebugOptions
    extends ExperimentalOptions, MemoryMonitorOptions, PipelineOptions {

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
   * Mapping of old PTransform names to new ones, specified as JSON <code>{"oldName":"newName",...}
   * </code>. To mark a transform as deleted, make newName the empty string.
   */
  @JsonIgnore
  @Description(
      "Mapping of old PTransform names to new ones, specified as JSON "
          + "{\"oldName\":\"newName\",...}. To mark a transform as deleted, make newName the empty "
          + "string.")
  Map<String, String> getTransformNameMapping();

  void setTransformNameMapping(Map<String, String> value);

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
   * If true, save a JFR profile when GC thrashing is first detected. The profile will run for the
   * amount of time set by --jfrRecordingDurationSec, or 60 seconds by default.
   *
   * <p>Note, JFR profiles are only supported on java 9 and up.
   */
  @Description(
      "If true, save a JFR profile before killing a thread or process "
          + "which is GC thrashing or out of memory.  Only available on java 9 or up")
  boolean getRecordJfrOnGcThrashing();

  void setRecordJfrOnGcThrashing(boolean value);

  @Default.Integer(60)
  @Description("The duration of the JFR recording taken if --recordJfrOnGcThrashing is set.")
  int getJfrRecordingDurationSec();

  void setJfrRecordingDurationSec(int value);

  /**
   * The size of the worker's in-memory cache, in megabytes.
   *
   * <p>Currently, this cache is used for storing read values of side inputs in batch as well as the
   * user state for streaming jobs.
   */
  @Description("The size of the worker's in-memory cache, in megabytes.")
  @Default.Integer(100)
  Integer getWorkerCacheMb();

  void setWorkerCacheMb(Integer value);

  /**
   * The amount of time before UnboundedReaders are considered idle and closed during streaming
   * execution.
   */
  @Description("The amount of time before UnboundedReaders are uncached, in seconds.")
  @Default.Integer(60)
  Integer getReaderCacheTimeoutSec();

  void setReaderCacheTimeoutSec(Integer value);

  /**
   * The max amount of time an UnboundedReader is consumed before checkpointing.
   *
   * @deprecated use {@link DataflowPipelineDebugOptions#getUnboundedReaderMaxReadTimeMs()} instead
   */
  @Description(
      "The max amount of time before an UnboundedReader is consumed before checkpointing, in seconds.")
  @Deprecated
  @Default.Integer(10)
  Integer getUnboundedReaderMaxReadTimeSec();

  void setUnboundedReaderMaxReadTimeSec(Integer value);

  /** The max amount of time an UnboundedReader is consumed before checkpointing. */
  @Description(
      "The max amount of time before an UnboundedReader is consumed before checkpointing, in millis.")
  @Default.InstanceFactory(UnboundedReaderMaxReadTimeFactory.class)
  Integer getUnboundedReaderMaxReadTimeMs();

  void setUnboundedReaderMaxReadTimeMs(Integer value);

  /**
   * Sets Integer value based on old, deprecated field ({@link
   * DataflowPipelineDebugOptions#getUnboundedReaderMaxReadTimeSec()}).
   */
  final class UnboundedReaderMaxReadTimeFactory implements DefaultValueFactory<Integer> {
    @Override
    public Integer create(PipelineOptions options) {
      DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
      return debugOptions.getUnboundedReaderMaxReadTimeSec() * 1000;
    }
  }

  /** The max elements read from an UnboundedReader before checkpointing. */
  @Description("The max elements read from an UnboundedReader before checkpointing. ")
  @Default.Integer(10 * 1000)
  Integer getUnboundedReaderMaxElements();

  void setUnboundedReaderMaxElements(Integer value);

  /** The max amount of time waiting for elements when reading from UnboundedReader. */
  @Description("The max amount of time waiting for elements when reading from UnboundedReader.")
  @Default.Integer(1000)
  Integer getUnboundedReaderMaxWaitForElementsMs();

  void setUnboundedReaderMaxWaitForElementsMs(Integer value);

  /**
   * The desired number of initial splits for UnboundedSources. If this value is <=0, the splits
   * will be computed based on the number of user workers.
   */
  @Description("The desired number of initial splits for UnboundedSources.")
  @Default.Integer(0)
  int getDesiredNumUnboundedSourceSplits();

  void setDesiredNumUnboundedSourceSplits(int value);

  /**
   * CAUTION: This option implies dumpHeapOnOOM, and has similar caveats. Specifically, heap dumps
   * can of comparable size to the default boot disk. Consider increasing the boot disk size before
   * setting this flag to true.
   */
  @Description(
      "Set to a GCS bucket (directory) to upload heap dumps to the given location.\n"
          + "Enabling this implies that heap dumps should be generated on OOM (--dumpHeapOnOOM=true)\n"
          + "Uploads will continue until the pipeline is stopped or updated without this option.\n")
  String getSaveHeapDumpsToGcsPath();

  void setSaveHeapDumpsToGcsPath(String gcsPath);

  /** Overrides for SDK harness container images. */
  @Description(
      "Overrides for SDK harness container images. Each entry consist of two values separated by \n"
          + "a comma where first value gives a regex to identify the container image to override \n"
          + "and the second value gives the replacement container image.")
  String getSdkHarnessContainerImageOverrides();

  void setSdkHarnessContainerImageOverrides(String value);

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
