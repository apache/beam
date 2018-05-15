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
package org.apache.beam.sdk.extensions.euphoria.executor.local;

import com.google.common.collect.Iterables;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ExternalIterable;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.SpillTools;
import org.apache.beam.sdk.extensions.euphoria.core.executor.util.InMemExternalIterable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** {@code SpillTools} that actually don't spill and use memory instead. */
class LocalSpillTools implements SpillTools {

  @Override
  public <T> ExternalIterable<T> externalize(Iterable<T> what) {
    return new InMemExternalIterable<>(what);
  }

  @Override
  public <T> Collection<ExternalIterable<T>> spillAndSortParts(
      Iterable<T> what, Comparator<T> comparator) {

    List<T> list = new ArrayList<>();
    Iterables.addAll(list, what);
    return Collections.singletonList(
        new InMemExternalIterable<>(list.stream().sorted(comparator).collect(Collectors.toList())));
  }
}
