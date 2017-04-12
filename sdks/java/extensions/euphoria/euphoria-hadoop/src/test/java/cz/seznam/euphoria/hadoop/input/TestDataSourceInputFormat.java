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
package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@code DataSourceInputFormat} test suite.
 */
public class TestDataSourceInputFormat {
  
  
  static class DummyPartition<T> implements Partition<T> {
    
    final Set<String> locations;
    final Iterable<T> data;
    
    DummyPartition(Set<String> locations, Iterable<T> data) {
      this.locations = locations;
      this.data = data;
    }

    @Override
    public Set<String> getLocations() {
      return locations;
    }

    @Override
    public Reader<T> openReader() throws IOException {
      return new Reader<T>() {

        Iterator<T> it = data.iterator();

        @Override
        public void close() throws IOException {
          // nop
        }

        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public T next() {
          return it.next();
        }

      };
    }
    
  }

  static class DummySource<T> implements DataSource<T> {

    final Supplier<T> supplier;

    DummySource(Supplier<T> supplier) {
      this.supplier = supplier;
    }

    @Override
    public List<Partition<T>> getPartitions() {
      return Arrays.asList(
          new DummyPartition<>(Sets.newHashSet("a", "b"), elements(2)),
          new DummyPartition<>(Sets.newHashSet("c", "d"), elements(3))
      );
    }

    @Override
    public boolean isBounded() {
      return false;
    }

    private List<T> elements(int count) {
      List<T> ret = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        ret.add(supplier.get());
      }
      return ret;
    }

    
  }

  @Test
  public void testDataSource() throws Exception {
    DummySource<Pair<Long, Long>> source = new DummySource<>(
        () -> Pair.of(
                Math.round(Math.random() * Long.MAX_VALUE),
                Math.round(Math.random() * Long.MAX_VALUE)));
    
    Configuration conf = new Configuration();
    TaskAttemptContext tac = mock(TaskAttemptContext.class);
    DataSourceInputFormat.configure(conf, source);
    
    when(tac.getConfiguration()).thenReturn(conf);

    InputFormat<NullWritable, Pair<Long, Long>> inputFormat = new DataSourceInputFormat<>();
    List<InputSplit> splits = inputFormat.getSplits(tac);
    assertEquals(2, splits.size());

    try (RecordReader<NullWritable, Pair<Long, Long>> reader = inputFormat.createRecordReader(
        splits.get(0), tac)) {
      reader.initialize(splits.get(0), tac);
      assertTrue(reader.nextKeyValue());
      reader.getCurrentKey();
      reader.getCurrentValue();
      assertTrue(reader.nextKeyValue());
      assertFalse(reader.nextKeyValue());
    }

    try (RecordReader<NullWritable, Pair<Long, Long>> reader = inputFormat.createRecordReader(
        splits.get(1), tac)) {
      reader.initialize(splits.get(1), tac);
      assertTrue(reader.nextKeyValue());
      reader.getCurrentKey();
      reader.getCurrentValue();
      assertTrue(reader.nextKeyValue());
      assertTrue(reader.nextKeyValue());
      assertFalse(reader.nextKeyValue());
    }

  }

}
