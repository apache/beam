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
package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import cz.seznam.euphoria.core.client.io.BoundedReader;
import cz.seznam.euphoria.core.client.io.UnsplittableBoundedSource;
import cz.seznam.euphoria.core.client.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import static java.util.stream.Collectors.toList;

/**
 * A convenience, easy-to-use data source reading hadoop based inputs
 * as lines of UTF-8 encoded text and delivering them as strings.
 */
public class SimpleHadoopTextFileSource implements BoundedDataSource<String> {

  /**
   * Wraps a {@code BoundedReader<Pair<LongWritable, Text>>} to provide an API as
   * {@code BoundedReader<String>} where the {@code Text} from the original reader
   * is transparently converted to a string.
   */
  static final class WrapReader implements BoundedReader<String> {
    private final BoundedReader<Pair<LongWritable, Text>> wrap;

    WrapReader(BoundedReader<Pair<LongWritable, Text>> wrap) {
      this.wrap = Objects.requireNonNull(wrap);
    }

    @Override
    public void close() throws IOException {
      this.wrap.close();
    }

    @Override
    public boolean hasNext() {
      return this.wrap.hasNext();
    }

    @Override
    public String next() {
      Pair<LongWritable, Text> p = this.wrap.next();
      return p.getSecond().toString();
    }
  }

  /**
   * Wraps a {@code BoundedPartition<Pair<LongWritable, Text>>} to provide an API as
   * {@code BoundedPartition<String>} where the {@code Text} is from the original partition
   * is transparently convered to a string.
   */
  static final class WrapPartition extends UnsplittableBoundedSource<String> {
    private final BoundedDataSource<Pair<LongWritable, Text>> wrap;

    WrapPartition(BoundedDataSource<Pair<LongWritable, Text>> wrap) {
      this.wrap = Objects.requireNonNull(wrap);
    }

    @Override
    public Set<String> getLocations() {
      return wrap.getLocations();
    }

    @Override
    public BoundedReader<String> openReader() throws IOException {
      return new WrapReader(this.wrap.openReader());
    }

  }

  private final HadoopTextFileSource wrap;


  @Override
  public List<BoundedDataSource<String>> split(long desiredSplitSize) {
    return this.wrap.split(desiredSplitSize).stream().map(WrapPartition::new).collect(toList());
  }

  @Override
  public boolean isBounded() {
    return this.wrap.isBounded();
  }

  @Override
  public Set<String> getLocations() {
    return Collections.singleton("unknown");
  }

  @Override
  public BoundedReader<String> openReader() throws IOException {
    throw new UnsupportedOperationException("Call `split` first!");
  }

  /**
   * Convenience constructor invoking
   * {@link #SimpleHadoopTextFileSource(String, Configuration)}
   * with a newly created hadoop configuration.
   *
   * @param path the path where to place the output to
   *
   * @throws NullPointerException if any of the given parameters is {@code null}
   */
  public SimpleHadoopTextFileSource(String path) {
    this(path, new Configuration());
  }

  /**
   * Constructs a data sink based on hadoop's {@link HadoopTextFileSource}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  public SimpleHadoopTextFileSource(String path, Configuration hadoopConfig) {
    this.wrap = new HadoopTextFileSource(path, hadoopConfig);
  }

}
