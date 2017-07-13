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
package org.apache.beam.runners.jstorm.util;

import org.apache.beam.runners.jstorm.translation.runtime.Executor;

import org.apache.beam.runners.jstorm.translation.runtime.GroupByWindowExecutor;
import org.apache.beam.runners.jstorm.translation.runtime.MultiStatefulDoFnExecutor;
import org.apache.beam.runners.jstorm.translation.runtime.StatefulDoFnExecutor;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

public class RunnerUtils {
    /**
     * Convert WindowedValue<KV<>> into KeyedWorkItem<K, WindowedValue<V>>
     * @param elem
     * @return
     */
    public static <K, V> KeyedWorkItem<K, V> toKeyedWorkItem(WindowedValue<KV<K, V>> elem) {
        WindowedValue<KV<K, V>> kvElem = (WindowedValue<KV<K, V>>) elem;
        SingletonKeyedWorkItem<K, V> workItem = SingletonKeyedWorkItem.of(
                kvElem.getValue().getKey(),
                kvElem.withValue(kvElem.getValue().getValue()));
        return workItem;
    }

    public static boolean isGroupByKeyExecutor (Executor executor) {
        if (executor instanceof GroupByWindowExecutor) {
            return true;
        } else if (executor instanceof StatefulDoFnExecutor ||
                executor instanceof MultiStatefulDoFnExecutor) {
            return true;
        } else {
            return false;
        }
    }
}