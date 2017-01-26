/**
 * Copyright 2016 Seznam a.s.
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
package cz.seznam.euphoria.flink.batch.io;

import cz.seznam.euphoria.core.client.io.Partition;
import org.apache.flink.core.io.LocatableInputSplit;

class PartitionWrapper<T> extends LocatableInputSplit {

  private final Partition<T> partition;

  public PartitionWrapper(int splitNumber, Partition<T> partition) {
    super(splitNumber, partition.getLocations().toArray(
            new String[partition.getLocations().size()]));

    this.partition = partition;
  }

  public Partition<T> getPartition() {
    return partition;
  }
}
