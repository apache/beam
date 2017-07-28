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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A Read.Bounded place holder {@link Operation} during pipeline translation.
 */
class ReadOperation<T> extends Operation<T> {
  private final BoundedSource<T> source;

  ReadOperation(BoundedSource<T> source) {
    super(1);
    this.source = checkNotNull(source, "source");
  }

  @Override
  public void process(WindowedValue elem) {
    throw new IllegalStateException(
        String.format("%s should not in execution graph.", this.getClass().getSimpleName()));
  }

  BoundedSource<?> getSource() {
    return source;
  }
}
