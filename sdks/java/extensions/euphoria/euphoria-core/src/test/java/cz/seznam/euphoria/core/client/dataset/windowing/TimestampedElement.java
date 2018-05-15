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
package cz.seznam.euphoria.core.client.dataset.windowing;

/** TODO: complete javadoc. */
class TimestampedElement<W extends Window, T> implements WindowedElement<W, T> {

  private final long timestamp;

  public TimestampedElement(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public W getWindow() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public T getElement() {
    throw new UnsupportedOperationException();
  }
}
