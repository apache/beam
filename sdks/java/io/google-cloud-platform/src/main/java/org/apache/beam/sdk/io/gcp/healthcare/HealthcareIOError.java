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
package org.apache.beam.sdk.io.gcp.healthcare;

/** Class for capturing errors on IO operations on Google Cloud Healthcare APIs resources. */
public class HealthcareIOError<T> {
  public T dataResource;
  public Exception error;

  private HealthcareIOError(T dataResource, Exception error) {
    this.dataResource = dataResource;
    this.error = error;
  }

  public Exception getError() {
    return error;
  }

  public T getDataResource() {
    return dataResource;
  }

  static <T> HealthcareIOError<T> of(T dataResource, Exception error) {
    return new HealthcareIOError(dataResource, error);
  }
}
