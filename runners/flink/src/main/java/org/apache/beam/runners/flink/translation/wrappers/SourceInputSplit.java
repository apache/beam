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
package org.apache.beam.runners.flink.translation.wrappers;

import org.apache.beam.sdk.io.Source;
import org.apache.flink.core.io.InputSplit;

/**
 * {@link org.apache.flink.core.io.InputSplit} for {@link
 * org.apache.beam.runners.flink.translation.wrappers.SourceInputFormat}. We pass the sharded Source
 * around in the input split because Sources simply split up into several Sources for sharding. This
 * is different to how Flink creates a separate InputSplit from an InputFormat.
 */
public class SourceInputSplit<T> implements InputSplit {

  private Source<T> source;
  private int splitNumber;

  public SourceInputSplit() {}

  public SourceInputSplit(Source<T> source, int splitNumber) {
    this.source = source;
    this.splitNumber = splitNumber;
  }

  @Override
  public int getSplitNumber() {
    return splitNumber;
  }

  public Source<T> getSource() {
    return source;
  }
}
