package org.apache.beam.runners.samza;

/** Different Samza execution environments that defines how the Samza job will be deployed. */
public enum SamzaExecutionEnvironment {
  /**
   * Runs the Samza job on the local machine with only one container. There is no coordination
   * required since there is only one container deployed in a single JVM. This setting is generally
   * used for development and testing.
   */
  LOCAL,

  /**
   * Submits and runs the Samza job on YARN, a remote clustered resource manager. Samza works with
   * the YARN to provision and coordinate resources for your application and run it across a cluster
   * of machines. It also handles failures of individual instances and automatically restarts them.
   */
  YARN,

  /**
   * Runs Samza job as a stand alone embedded library mode which can be imported into your Java
   * application. You can increase your application’s capacity by spinning up multiple instances.
   * These instances will then dynamically coordinate with each other and distribute work among
   * themselves. If an instance fails, the tasks running on it will be re-assigned to the remaining
   * ones. By default, Samza uses Zookeeper for coordination across individual instances.
   */
  STANDALONE
}
