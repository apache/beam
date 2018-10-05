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


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A mock factory creating a stream datasource.
 */
public class MockStreamDataSource<T>
    implements UnboundedDataSource<T, MockStreamDataSource.Offset> {

  public static final class Offset implements Serializable {
    public static Offset get() { return null; }
  }

  private final List<UnboundedPartition<T, Offset>> partitions;

  public MockStreamDataSource() {
    final int p = 4;
    this.partitions = new ArrayList<>(p);
    for (int i = 0; i < p; i++) {
      partitions.add((UnboundedPartition<T, Offset>) () -> new EmptyReader<>());
    }
  }

  @Override
  public List<UnboundedPartition<T, Offset>> getPartitions() {
    return partitions;
  }

}
