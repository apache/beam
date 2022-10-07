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
package org.apache.beam.sdk.io.aws2.kinesis;

import java.util.concurrent.Callable;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;

// CHECKSTYLE.OFF: JavadocMethod
public class ErrorsUtils {
  /**
   * Wraps Amazon specific exceptions into more friendly format.
   *
   * @throws TransientKinesisException - in case of recoverable situation, i.e. the request rate is
   *     too high, Kinesis remote service failed, network issue, etc.
   * @throws ExpiredIteratorException - if iterator needs to be refreshed
   * @throws RuntimeException - in all other cases
   */
  public static <T> T wrapExceptions(Callable<T> callable) throws TransientKinesisException {
    try {
      return callable.call();
    } catch (ExpiredIteratorException e) {
      throw e;
    } catch (LimitExceededException | ProvisionedThroughputExceededException e) {
      throw new KinesisClientThrottledException(
          "Too many requests to Kinesis. Wait some time and retry.", e);
    } catch (SdkServiceException e) {
      if (e.isThrottlingException()
          || SdkDefaultRetrySetting.RETRYABLE_STATUS_CODES.contains(e.statusCode())) {
        throw new TransientKinesisException("Kinesis backend failed. Wait some time and retry.", e);
      }
      throw e; // others, such as 4xx, are not retryable
    } catch (SdkClientException e) {
      if (SdkDefaultRetrySetting.RETRYABLE_EXCEPTIONS.contains(e.getClass())) {
        throw new TransientKinesisException("Retryable failure", e);
      }
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Unknown kinesis failure, when trying to reach kinesis", e);
    }
  }
}
