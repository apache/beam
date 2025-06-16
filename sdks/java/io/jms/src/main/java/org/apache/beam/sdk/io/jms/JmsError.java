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
package org.apache.beam.sdk.io.jms;

import java.io.Serializable;

/**
 * A class to hold a record that failed to be written to JMS, and the exception that caused the
 * failure.
 */
public class JmsError<T> implements Serializable {
  private final T record;
  private final Exception exception;

  public JmsError(T record, Exception exception) {
    this.record = record;
    this.exception = exception;
  }

  public T getRecord() {
    return record;
  }

  public Exception getException() {
    return exception;
  }
}
