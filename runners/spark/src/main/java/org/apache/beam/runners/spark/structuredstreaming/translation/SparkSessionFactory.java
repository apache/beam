package org.apache.beam.runners.spark.structuredstreaming.translation;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory {

  /**
   * Gets active {@link SparkSession} or creates one using {@link
   * SparkStructuredStreamingPipelineOptions}.
   */
  public static SparkSession getOrCreateSession(SparkStructuredStreamingPipelineOptions options) {
    if (options.getUseActiveSparkSession()) {
      return SparkSession.active();
    }
    return sessionBuilder(options.getSparkMaster(), options.getAppName(), options.getFilesToStage())
        .getOrCreate();
  }

  /** Creates Spark session builder with some optimizations for local mode, e.g. in tests. */
  public static SparkSession.Builder sessionBuilder(String master) {
    return sessionBuilder(master, null, null);
  }

  private static SparkSession.Builder sessionBuilder(
      String master, @Nullable String appName, @Nullable List<String> jars) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster(master);
    if (appName != null) {
      sparkConf.setAppName(appName);
    }
    if (jars != null && !jars.isEmpty()) {
      sparkConf.setJars(jars.toArray(new String[0]));
    }

    // By default, Spark defines 200 as a number of sql partitions. This seems too much for local
    // mode, so try to align with value of "sparkMaster" option in this case.
    // We should not overwrite this value (or any user-defined spark configuration value) if the
    // user has already configured it.
    if (master != null
        && master.startsWith("local[")
        && System.getProperty("spark.sql.shuffle.partitions") == null) {
      int numPartitions =
          Integer.parseInt(master.substring("local[".length(), master.length() - 1));
      if (numPartitions > 0) {
        sparkConf.set("spark.sql.shuffle.partitions", String.valueOf(numPartitions));
      }
    }
    return SparkSession.builder().config(sparkConf);
  }
}
