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
package org.apache.beam.sdk.extensions.smb;

import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TupleTag;

/** Represents the result of an SMB co-group operation. */
public class SMBCoGbkResult {
  private final Map<TupleTag, Iterable<?>> valueMap;

  SMBCoGbkResult(Map<TupleTag, Iterable<?>> valueMap) {
    this.valueMap = valueMap;
  }

  public <V> Iterable<V> getValuesForTag(TupleTag<V> tag) {
    return (Iterable<V>) valueMap.get(tag);
  }

  /**
   * Serializable converter from SMBCoGbkResult to desired result type.
   *
   * @param <ResultT>
   */
  public abstract static class ToFinalResult<ResultT>
      implements SerializableFunction<SMBCoGbkResult, ResultT> {
    public abstract Coder<ResultT> resultCoder();
  }
}
