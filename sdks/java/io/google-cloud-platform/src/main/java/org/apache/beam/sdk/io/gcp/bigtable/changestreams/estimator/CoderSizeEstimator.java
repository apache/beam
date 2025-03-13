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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

/**
 * This class is used to estimate the size in bytes of a given element. It uses the given {@link
 * Coder} to calculate the size of the element.
 */
@Internal
public class CoderSizeEstimator<T> implements SizeEstimator<T>, Serializable {

  private static final long serialVersionUID = 5564948506493524158L;

  private static class SizeEstimatorObserver extends ElementByteSizeObserver
      implements Serializable {

    private static final long serialVersionUID = 4569562919962045617L;
    private long observedBytes;

    @Override
    protected void reportElementSize(long elementByteSize) {
      observedBytes = elementByteSize;
    }
  }

  private final Coder<T> coder;
  private final SizeEstimatorObserver observer;

  public CoderSizeEstimator(Coder<T> coder) {
    this.coder = coder;
    this.observer = new SizeEstimatorObserver();
  }

  /**
   * Estimates the size in bytes of the given element with the configured {@link Coder} .
   *
   * @param element the element instance to be estimated
   * @return the estimated size in bytes of the given element
   */
  @Override
  public long sizeOf(T element) {
    try {
      coder.registerByteSizeObserver(element, observer);
      observer.advance();

      return observer.observedBytes;
    } catch (Exception e) {
      throw new EncodingException(e);
    }
  }
}
