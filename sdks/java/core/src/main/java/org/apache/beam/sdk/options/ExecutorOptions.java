package org.apache.beam.sdk.options;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;

public interface ExecutorOptions extends PipelineOptions{

  /**
   * The ScheduledExecutorService instance to use to create threads, can be overridden to specify a
   * ScheduledExecutorService that is compatible with the user's environment. If unset, the default
   * is to create an ScheduledExecutorService with a core number of threads equal to Math.max(4,
   * Runtime.getRuntime().availableProcessors())
   */
  @JsonIgnore
  @Description(
      "The ScheduledExecutorService instance to use to create threads, can be overridden to specify "
          + "a ScheduledExecutorService that is compatible with the user's environment. If unset, "
          + "the default is to create a ScheduledExecutorService with a core number of threads "
          + "equal to Math.max(4, Runtime.getRuntime().availableProcessors())")
  @Default.InstanceFactory(ScheduledExecutorServiceFactory.class)
  @Hidden
  ScheduledExecutorService getScheduledExecutorService();

  void setScheduledExecutorService(ScheduledExecutorService value);


  /**
   * Returns the default {@link ScheduledExecutorService} to use within the Apache Beam SDK.
   */
  class ScheduledExecutorServiceFactory implements DefaultValueFactory<ScheduledExecutorService> {
    @SuppressWarnings("deprecation") // IS_APP_ENGINE is deprecated for internal use only.
    @Override
    public ScheduledExecutorService create(PipelineOptions options) {
      ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder();
      threadFactoryBuilder.setThreadFactory(MoreExecutors.platformThreadFactory());
      threadFactoryBuilder.setDaemon(true);
      /* The SDK requires an unbounded thread pool because a step may create X writers
       * each requiring their own thread to perform the writes otherwise a writer may
       * block causing deadlock for the step because the writers buffer is full.
       * Also, the MapTaskExecutor launches the steps in reverse order and completes
       * them in forward order thus requiring enough threads so that each step's writers
       * can be active.
       */

      /*The minimum of max(4, processors) was chosen as a default working configuration found in
      * the Bigquery client library
      */
      return Executors.newScheduledThreadPool(
          Math.max(4, Runtime.getRuntime().availableProcessors()), threadFactoryBuilder.build());
    }
  }

}
