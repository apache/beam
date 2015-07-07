/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.Preconditions;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An implementation of {@link BackOff} that limits the number of calls on another {@link BackOff}.
 * This class will call the same methods of another BackOff until the maximum number of
 * retries are reached, and then it will return {@codeBackOff.STOP}.
 */
@NotThreadSafe
public class RetryBoundedBackOff implements BackOff {
  private int retriesAttempted = 0;
  private final int maxRetries;
  private BackOff backoff;

  /**
   * RetryBoundedBackOff takes a {@link BackOff} and limits the retries.
   *
   * @param maxRetries Number of retries to attempt. Must be greater or equal to 0.
   * @param backoff The underlying {@link BackOff} to use.
   */
  public RetryBoundedBackOff(int maxRetries, BackOff backoff) {
    Preconditions.checkArgument(maxRetries >= 0,
        "Maximum number of retries must not be less than 0.");
    this.backoff = backoff;
    this.maxRetries = maxRetries;
  }

  @Override
  public void reset() throws IOException {
    backoff.reset();
    retriesAttempted = 0;
  }

  @Override
  public long nextBackOffMillis() throws IOException {
    if (retriesAttempted >= maxRetries) {
      return BackOff.STOP;
    }
    long next = backoff.nextBackOffMillis();
    if (next == BackOff.STOP) {
      return BackOff.STOP;
    }
    retriesAttempted++;
    return next;
  }
}
