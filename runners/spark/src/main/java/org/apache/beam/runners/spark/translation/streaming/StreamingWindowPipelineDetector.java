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

package org.apache.beam.runners.spark.translation.streaming;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;


/**
 * Pipeline {@link SparkRunner.Evaluator} to detect windowing.
 */
public final class StreamingWindowPipelineDetector extends SparkRunner.Evaluator {

  // Currently, Spark streaming recommends batches no smaller then 500 msec
  private static final Duration SPARK_MIN_WINDOW = Durations.milliseconds(500);

  private boolean windowing;
  private Duration batchDuration;

  public StreamingWindowPipelineDetector(SparkPipelineTranslator translator) {
    super(translator);
  }

  private static final TransformTranslator.FieldGetter WINDOW_FG =
      new TransformTranslator.FieldGetter(Window.Bound.class);

  // Use the smallest window (fixed or sliding) as Spark streaming's batch duration
  @Override
  protected <TransformT extends PTransform<? super PInput, POutput>> void
      doVisitTransform(TransformTreeNode node) {
    @SuppressWarnings("unchecked")
    TransformT transform = (TransformT) node.getTransform();
    @SuppressWarnings("unchecked")
    Class<TransformT> transformClass = (Class<TransformT>) (Class<?>) transform.getClass();
    if (transformClass.isAssignableFrom(Window.Bound.class)) {
      WindowFn<?, ?> windowFn = WINDOW_FG.get("windowFn", transform);
      if (windowFn instanceof FixedWindows) {
        setBatchDuration(((FixedWindows) windowFn).getSize());
      } else if (windowFn instanceof SlidingWindows) {
        if (((SlidingWindows) windowFn).getOffset().getMillis() > 0) {
          throw new UnsupportedOperationException("Spark does not support window offsets");
        }
        // Sliding window size might as well set the batch duration. Applying the transformation
        // will add the "slide"
        setBatchDuration(((SlidingWindows) windowFn).getSize());
      } else if (!(windowFn instanceof GlobalWindows)) {
        throw new IllegalStateException("Windowing function not supported: " + windowFn);
      }
    }
  }

  private void setBatchDuration(org.joda.time.Duration duration) {
    Long durationMillis = duration.getMillis();
    // validate window size
    if (durationMillis < SPARK_MIN_WINDOW.milliseconds()) {
      throw new IllegalArgumentException("Windowing of size " + durationMillis
          + "msec is not supported!");
    }
    // choose the smallest duration to be Spark's batch duration, larger ones will be handled
    // as window functions  over the batched-stream
    if (!windowing || this.batchDuration.milliseconds() > durationMillis) {
      this.batchDuration = Durations.milliseconds(durationMillis);
    }
    windowing = true;
  }

  public boolean isWindowing() {
    return windowing;
  }

  public Duration getBatchDuration() {
    return batchDuration;
  }
}
