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
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.util.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

class Util {

  static <T> List<T> sorted(Collection<T> xs, Comparator<T> c) {
    ArrayList<T> list = new ArrayList<>(xs);
    list.sort(c);
    return list;
  }

  static <T extends Comparable<T>> List<T> sorted(Collection<T> xs) {
    return sorted(xs, Comparator.naturalOrder());
  }

  @SuppressWarnings("unchecked")
  static <T, W extends Window> Dataset<Pair<W, T>> extractWindow(Dataset<T> input) {
    return MapElements.of(input).using((e, ctx) -> Pair.of((W) ctx.getWindow(), e)).output();
  }
}
