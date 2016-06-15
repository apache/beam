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

import org.apache.beam.sdk.util.WindowedValue;

/**
 * A {@link TransformEvaluator} which delegates calls to an underlying {@link TransformEvaluator},
 * clearing the value of a {@link ThreadLocal} if any call throws an exception.
 */
class ThreadLocalInvalidatingTransformEvaluator<InputT>
    implements TransformEvaluator<InputT> {
  private final TransformEvaluator<InputT> underlying;
  private final ThreadLocal<?> threadLocal;

  public static <InputT> TransformEvaluator<InputT> wrapping(
      TransformEvaluator<InputT> underlying,
      ThreadLocal<?> threadLocal) {
    return new ThreadLocalInvalidatingTransformEvaluator<>(underlying, threadLocal);
  }

  private ThreadLocalInvalidatingTransformEvaluator(
      TransformEvaluator<InputT> underlying, ThreadLocal<?> threadLocal) {
    this.underlying = underlying;
    this.threadLocal = threadLocal;
  }

  @Override
  public void processElement(WindowedValue<InputT> element) throws Exception {
    try {
      underlying.processElement(element);
    } catch (Exception e) {
      threadLocal.remove();
      throw e;
    }
  }

  @Override
  public TransformResult finishBundle() throws Exception {
    try {
      return underlying.finishBundle();
    } catch (Exception e) {
      threadLocal.remove();
      throw e;
    }
  }
}
