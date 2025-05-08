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
package org.apache.beam.runners.spark.translation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.runners.spark.SparkContextRule;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Option;

/**
 * Tests for {@link SingleEmitInputDStream} class which ensures data is emitted exactly once in
 * Spark streaming context.
 */
public class SingleEmitInputDStreamTest implements Serializable {
  @ClassRule public static SparkContextRule sparkContext = new SparkContextRule();
  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final Duration checkpointDuration = new Duration(500L);

  /** Creates a temporary directory for storing checkpoints before each test execution. */
  @Before
  public void init() {
    try {
      temporaryFolder.create();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void singleEmitInputDStreamShouldBeEmitOnlyOnce() {
    // Initialize Spark contexts
    JavaSparkContext jsc = sparkContext.getSparkContext();
    JavaStreamingContext jssc = new JavaStreamingContext(jsc, checkpointDuration);

    // Create test data and wrap it in SingleEmitInputDStream
    final SingleEmitInputDStream<String> singleEmitInputDStream = singleEmitInputDSTream(jsc, jssc);
    singleEmitInputDStream.checkpoint(checkpointDuration);

    // First computation: should return the original data
    Option<RDD<String>> rddOption = singleEmitInputDStream.compute(null);
    RDD<String> rdd = rddOption.get();
    JavaRDD<String> javaRDD = JavaRDD.fromRDD(rdd, JavaSparkContext$.MODULE$.fakeClassTag());
    List<String> collect = javaRDD.collect();
    assertNotNull(collect);
    assertThat(collect, hasItems("foo", "bar"));

    // Second computation: should return empty RDD since data was already emitted
    rddOption = singleEmitInputDStream.compute(null);
    rdd = rddOption.get();
    javaRDD = JavaRDD.fromRDD(rdd, JavaSparkContext$.MODULE$.fakeClassTag());
    collect = javaRDD.collect();
    assertNotNull(collect);
    assertThat(collect, empty());
  }

  private SingleEmitInputDStream<String> singleEmitInputDSTream(
      JavaSparkContext jsc, JavaStreamingContext jssc) {
    final JavaRDD<String> stringRDD = jsc.parallelize(Lists.newArrayList("foo", "bar"));
    final SingleEmitInputDStream<String> singleEmitInputDStream =
        new SingleEmitInputDStream<>(jssc.ssc(), stringRDD.rdd());
    return singleEmitInputDStream;
  }

  @Test
  public void singleEmitInputDStreamShouldBeEmptyAfterCheckpointRecovery()
      throws InterruptedException {
    // Set up checkpoint directory
    String checkpointPath = temporaryFolder.getRoot().getPath();

    // Initialize Spark contexts with checkpoint configuration
    JavaSparkContext jsc = sparkContext.getSparkContext();
    jsc.setCheckpointDir(checkpointPath);
    JavaStreamingContext jssc = new JavaStreamingContext(jsc, checkpointDuration);
    jssc.checkpoint(checkpointPath);

    // Create test data and configure SingleEmitInputDStream
    final SingleEmitInputDStream<String> singleEmitInputDStream = singleEmitInputDSTream(jsc, jssc);
    singleEmitInputDStream.checkpoint(checkpointDuration);

    // Register output operation required by Spark Streaming
    singleEmitInputDStream.print();

    // Compute initial RDD and verify original data is present
    Option<RDD<String>> rddOption = singleEmitInputDStream.compute(null);
    RDD<String> rdd = rddOption.get();

    JavaRDD<String> javaRDD = JavaRDD.fromRDD(rdd, JavaSparkContext$.MODULE$.fakeClassTag());
    List<String> collect = javaRDD.collect();
    assertNotNull(collect);
    assertThat(collect, hasItems("foo", "bar"));

    // Start streaming context to create checkpoint data
    jssc.start();
    // Wait for checkpoint to be created and written
    jssc.awaitTerminationOrTimeout(1000);
    // Ensure clean shutdown and checkpoint writing
    jssc.stop(true, true);

    // Recover streaming context from checkpoint
    JavaStreamingContext recoveredJssc =
        JavaStreamingContext.getOrCreate(
            checkpointPath,
            () -> {
              throw new RuntimeException(
                  "Should not create new context, should recover from checkpoint");
            });

    try {
      // Extract recovered DStream from the restored context
      @SuppressWarnings("unchecked")
      SingleEmitInputDStream<String> recoveredDStream =
          (SingleEmitInputDStream<String>) recoveredJssc.ssc().graph().getInputStreams()[0];

      // Compute RDD from recovered DStream and verify it's empty
      Option<RDD<String>> recoveredRddOption = recoveredDStream.compute(null);
      RDD<String> recoveredRdd = recoveredRddOption.get();
      JavaRDD<String> recoveredJavaRdd =
          JavaRDD.fromRDD(recoveredRdd, JavaSparkContext$.MODULE$.fakeClassTag());
      List<String> recoveredCollect = recoveredJavaRdd.collect();

      // Verify that recovered DStream produces empty results
      assertNotNull(recoveredCollect);
      assertThat(recoveredCollect, empty());

    } finally {
      // Ensure recovered context is properly cleaned up
      recoveredJssc.stop(true, true);
    }
  }
}
