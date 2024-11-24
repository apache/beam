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
package org.apache.beam.sdk.io.common;

import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.testing.TestPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Methods common to all types of IOITs. */
public class IOITHelper {
  private static final Logger LOG = LoggerFactory.getLogger(IOITHelper.class);
  private static final int maxAttempts = 3;
  private static final long minDelay = 1_000;

  private IOITHelper() {}

  public static String getHashForRecordCount(int recordCount, Map<Integer, String> hashes) {
    String hash = hashes.get(recordCount);
    if (hash == null) {
      throw new UnsupportedOperationException(
          String.format("No hash for that record count: %s", recordCount));
    }
    return hash;
  }

  public static <T extends PipelineOptions> T readIOTestPipelineOptions(Class<T> optionsType) {

    PipelineOptionsFactory.register(optionsType);
    PipelineOptions options = TestPipeline.testingPipelineOptions().as(optionsType);

    return PipelineOptionsValidator.validate(optionsType, options);
  }

  /** Interface for passing any method to executeWithRetry function. */
  @FunctionalInterface
  public interface RetryFunction {
    void run() throws Exception;
  }

  /**
   * This function executes the method and retries it in case of failure.
   *
   * @param function The function to retry
   * @throws Exception
   */
  public static void executeWithRetry(RetryFunction function) throws Exception {
    executeWithRetry(maxAttempts, minDelay, function);
  }

  /**
   * This function executes the method and retries it in case of failure. The method is retried when
   * an exception is thrown and it does not depend on the error response. This can be used for tests
   * which await for infrastructure setup i.e. database connection.
   *
   * @param maxAttempts The number of retry attempts
   * @param minDelay Minimal delay which will grow exponentially
   * @param function The function to retry
   * @throws Exception
   */
  public static void executeWithRetry(int maxAttempts, long minDelay, RetryFunction function)
      throws Exception {
    int attempts = 1;
    long delay = minDelay;

    while (attempts <= maxAttempts) {
      try {
        function.run();
        return;
      } catch (Exception e) {
        LOG.warn("Attempt #{} of {} failed: {}.", attempts, maxAttempts, e.getMessage());
        if (attempts == maxAttempts) {
          throw e;
        } else {
          long nextDelay = (long) Math.pow(2, attempts) * delay;
          LOG.warn("Retrying in {} ms.", nextDelay);
          Thread.sleep(nextDelay);
        }
        attempts++;
      }
    }
  }
}
