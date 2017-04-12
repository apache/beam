/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.testkit;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.stream.IntStream;

class ModuloInputSplitAssigner implements InputSplitAssigner {

  private final Deque<InputSplit>[] partitions;

  @SuppressWarnings("unchecked")
  public ModuloInputSplitAssigner(InputSplit[] splits, int partitions) {
    this.partitions = IntStream.range(0, partitions)
        .mapToObj(i -> new ArrayDeque<>())
        .toArray(Deque[]::new);
    for (int i = 0; i < splits.length; i++) {
      this.partitions[i % partitions].add(splits[i]);
    }
  }

  @Override
  public InputSplit getNextInputSplit(String host, int taskId) {
    synchronized (this.partitions) {
      return this.partitions[taskId].poll();
    }
  }
}
