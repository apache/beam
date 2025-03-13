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
package org.apache.beam.io.requestresponse;

/**
 * A {@link UserCodeExecutionException} that signals an error with a remote system. Examples of such
 * errors include an HTTP 5XX error or gRPC INTERNAL (13) error.
 */
public class UserCodeRemoteSystemException extends UserCodeExecutionException {
  public UserCodeRemoteSystemException(String message) {
    super(message);
  }

  public UserCodeRemoteSystemException(String message, Throwable cause) {
    super(message, cause);
  }

  public UserCodeRemoteSystemException(Throwable cause) {
    super(cause);
  }

  public UserCodeRemoteSystemException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  /**
   * Reports that remote system errors should be repeated. These may be transient errors of a remote
   * API service that resolve in time. Thus requests should be repeated..
   */
  @Override
  public boolean shouldRepeat() {
    return true;
  }
}
