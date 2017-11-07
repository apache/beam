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
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import java.util.List;

/**
 * A {@code DataSource} with bounded data.
 */
@Audience(Audience.Type.EXECUTOR)
public interface BoundedDataSource<T> extends DataSource<T> {

  /** @return a list of all partitions of this source */
  List<BoundedPartition<T>> getPartitions();


  @Override
  public default boolean isBounded() {
    return true;
  }

  @Override
  default BoundedDataSource<T> asBounded() {
    return this;
  }

  @Override
  default int getParallelism() {
    return getPartitions().size();
  }

}
