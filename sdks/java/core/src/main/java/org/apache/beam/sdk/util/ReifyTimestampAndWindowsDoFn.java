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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.OldDoFn.RequiresWindowAccess;
import org.apache.beam.sdk.values.KV;

/**
 * {@link OldDoFn} that makes timestamps and window assignments explicit in the value part of each
 * key/value pair.
 *
 * @param <K> the type of the keys of the input and output {@code PCollection}s
 * @param <V> the type of the values of the input {@code PCollection}
 */
@SystemDoFnInternal
public class ReifyTimestampAndWindowsDoFn<K, V> extends OldDoFn<KV<K, V>, KV<K, WindowedValue<V>>>
    implements RequiresWindowAccess {
  @Override
  public void processElement(ProcessContext c) throws Exception {
    KV<K, V> kv = c.element();
    K key = kv.getKey();
    V value = kv.getValue();
    c.output(KV.of(key, WindowedValue.of(value, c.timestamp(), c.window(), c.pane())));
  }
}
