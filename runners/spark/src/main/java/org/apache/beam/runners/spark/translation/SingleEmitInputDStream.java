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

import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.ConstantInputDStream;
import org.apache.spark.streaming.dstream.QueueInputDStream;
import scala.Option;

/**
 * A specialized {@link ConstantInputDStream} that emits its RDD exactly once. Alternative to {@link
 * QueueInputDStream} when checkpointing is required.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Supports checkpointing
 *   <li>Guarantees single emission of data
 *   <li>Returns empty RDD after first emission
 * </ul>
 *
 * @param <T> The type of elements in the RDD
 */
public class SingleEmitInputDStream<T> extends ConstantInputDStream<T> {

  private boolean emitted = false;
  private final boolean forceWatermarkSync;
  private transient long lastValidTimeMs = 0;

  public SingleEmitInputDStream(StreamingContext ssc, RDD<T> rdd) {
    this(ssc, rdd, false);
  }

  public SingleEmitInputDStream(StreamingContext ssc, RDD<T> rdd, boolean forceWatermarkSync) {
    super(ssc, rdd, JavaSparkContext$.MODULE$.fakeClassTag());
    this.forceWatermarkSync = forceWatermarkSync;
  }

  @Override
  public Option<RDD<T>> compute(Time validTime) {
    if (forceWatermarkSync && validTime != null && validTime.milliseconds() > lastValidTimeMs) {
      waitForLastBatch(validTime);
    }
    if (this.emitted) {
      return Option.apply(this.emptyRDD());
    } else {
      this.emitted = true;
      return super.compute(validTime);
    }
  }

  private void waitForLastBatch(Time validTime) {
    while (GlobalWatermarkHolder.hasWatermarks()
        && GlobalWatermarkHolder.getLastWatermarkedBatchTime() < lastValidTimeMs) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    lastValidTimeMs = validTime.milliseconds();
  }

  private RDD<T> emptyRDD() {
    return this.context().sparkContext().emptyRDD(JavaSparkContext$.MODULE$.fakeClassTag());
  }
}
