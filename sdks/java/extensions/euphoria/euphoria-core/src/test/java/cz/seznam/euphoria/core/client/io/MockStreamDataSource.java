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

import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A mock factory creating a stream datasource.
 */
public class MockStreamDataSource<T> implements DataSource<T> {

  private final List<Partition<T>> partitions;

  public MockStreamDataSource() {
    final int p = 4;
    final List<Partition<T>> partitions = new ArrayList<>(p);
    for (int i = 0; i < p; i++) {
      partitions.add(new Partition<T>() {

        @Override
        public Set<String> getLocations() {
          return Sets.newHashSet("localhost");
        }

        @Override
        public Reader<T> openReader() throws IOException {
          return new EmptyReader<>();
        }
      });
    }
    this.partitions = partitions;
  }

  @Override
  public List<Partition<T>> getPartitions() {
    return partitions;
  }

  @Override
  public boolean isBounded() {
    return false;
  }
}
