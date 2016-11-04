package cz.seznam.euphoria.spark;

import org.apache.spark.SparkConf;

/**
 * Executor running Spark in "local environment". The local execution environment
 * will run the program in a multi-threaded fashion in the same JVM as the
 * environment was created in. Level of parallelism can be optionally
 * set using constructor parameter.
 */
public class TestSparkExecutor extends SparkExecutor {

  public TestSparkExecutor(int parallelism) {
    super(new SparkConf()
            .setMaster("local[" + parallelism + "]")
            .setAppName("test")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"));
  }

  /**
   * Creates {@link TestSparkExecutor} with default parallelism of 8.
   */
  public TestSparkExecutor() {
    this(8);
  }
}
