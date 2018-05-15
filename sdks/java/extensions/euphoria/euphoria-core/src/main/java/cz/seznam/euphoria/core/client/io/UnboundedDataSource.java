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
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import java.io.Serializable;
import java.util.List;

/**
 * {@code DataSource} for unbounded data.
 *
 * @param <T> the data type
 * @param <OffsetT> the type of object that is being used to track progress of the source. The
 *     object has to be serializable, because java serialization is being used for checkpointing the
 *     state.
 */
@Audience(Audience.Type.EXECUTOR)
public interface UnboundedDataSource<T, OffsetT extends Serializable> extends DataSource<T> {

  /**
   * Error should be reported via throwing RuntimeException (Do not return empty list on error).
   *
   * @return a list of all partitions of this source
   */
  List<UnboundedPartition<T, OffsetT>> getPartitions();

  @Override
  default boolean isBounded() {
    return false;
  }

  @Override
  default UnboundedDataSource<T, OffsetT> asUnbounded() {
    return this;
  }
}
