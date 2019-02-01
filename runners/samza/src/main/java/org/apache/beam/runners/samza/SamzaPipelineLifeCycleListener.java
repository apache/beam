package org.apache.beam.runners.samza;

import org.apache.samza.config.Config;

/** Life cycle listener for a Samza pipeline during runtime. */
public interface SamzaPipelineLifeCycleListener {

  /** Callback when the pipeline is started. */
  void onStart(Config config);

  /**
   * Callback after the pipeline is submmitted. This will be invoked only for Samza jobs submitted
   * to a cluster.
   */
  void onSubmit();

  /** Callback after the pipeline is finished. */
  void onFinish();

  /** A registrar for {@link SamzaPipelineLifeCycleListener}. */
  interface Registrar {
    SamzaPipelineLifeCycleListener getLifeCycleListener();
  }
}
