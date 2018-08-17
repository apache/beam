/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.hint.OutputHint;
import java.util.ArrayList;
import java.util.List;

/** Utility class for easier creating input datasets for operator testing. */
public class Util {

  public static <T> Dataset<T> createMockDataset(Flow flow, int numPartitions) {
    @SuppressWarnings("unchecked")
    List<T>[] partitions = new List[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      partitions[i] = new ArrayList<>();
    }

    return flow.createInput(ListDataSource.bounded(partitions));
  }

  /** Empty implementation of OutputHint. */
  public static class TestHint implements OutputHint {

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TestHint;
    }
  }

  /** Empty implementation of OutputHint. */
  public static class TestHint2 implements OutputHint {

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TestHint2;
    }
  }
}
