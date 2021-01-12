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
package org.apache.beam.runners.direct;

import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransformEvaluator} which delegates calls to an underlying {@link TransformEvaluator},
 * clearing the value of a {@link DoFnLifecycleManager} if any call throws an exception.
 */
class DoFnLifecycleManagerRemovingTransformEvaluator<InputT> implements TransformEvaluator<InputT> {
  private static final Logger LOG =
      LoggerFactory.getLogger(DoFnLifecycleManagerRemovingTransformEvaluator.class);
  private final ParDoEvaluator<InputT> underlying;
  private final DoFnLifecycleManager lifecycleManager;

  public static <InputT> DoFnLifecycleManagerRemovingTransformEvaluator<InputT> wrapping(
      ParDoEvaluator<InputT> underlying, DoFnLifecycleManager lifecycleManager) {
    return new DoFnLifecycleManagerRemovingTransformEvaluator<>(underlying, lifecycleManager);
  }

  private DoFnLifecycleManagerRemovingTransformEvaluator(
      ParDoEvaluator<InputT> underlying, DoFnLifecycleManager lifecycleManager) {
    this.underlying = underlying;
    this.lifecycleManager = lifecycleManager;
  }

  public ParDoEvaluator<InputT> getParDoEvaluator() {
    return underlying;
  }

  @Override
  public void processElement(WindowedValue<InputT> element) throws Exception {
    try {
      underlying.processElement(element);
    } catch (Exception e) {
      onException(e, "Exception encountered while cleaning up after processing an element");
      throw e;
    }
  }

  public <KeyT> void onTimer(TimerData timer, KeyT key, BoundedWindow window) throws Exception {
    try {
      underlying.onTimer(timer, key, window);
    } catch (Exception e) {
      onException(e, "Exception encountered while cleaning up after processing a timer");
      throw e;
    }
  }

  @Override
  public TransformResult<InputT> finishBundle() throws Exception {
    try {
      return underlying.finishBundle();
    } catch (Exception e) {
      onException(e, "Exception encountered while cleaning up after finishing a bundle");
      throw e;
    }
  }

  private void onException(Exception e, String msg) {
    try {
      lifecycleManager.remove();
    } catch (Exception removalException) {
      if (removalException instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      LOG.error(msg, removalException);
      e.addSuppressed(removalException);
    }
  }
}
