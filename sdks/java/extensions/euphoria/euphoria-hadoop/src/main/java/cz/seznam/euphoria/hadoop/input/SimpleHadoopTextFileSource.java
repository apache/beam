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
package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.DataSourceFactory;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toList;

/**
 * A convenience, easy-to-use data source reading hadoop based inputs
 * as lines of UTF-8 encoded text and delivering them as strings.
 */
public class SimpleHadoopTextFileSource implements DataSource<String> {

  /**
   * Wraps a {@code Reader<Pair<LongWritable, Text>>} to provide an API as
   * {@code Reader<String>} where the {@code Text} from the original reader
   * is transparently converted to a string.
   */
  static final class WrapReader implements Reader<String> {
    private final Reader<Pair<LongWritable, Text>> wrap;

    WrapReader(Reader<Pair<LongWritable, Text>> wrap) {
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
   * Wraps a {@code Partition<Pair<LongWritable, Text>>} to provide an API as
   * {@code Partition<String>} where the {@code Text} is from the original partition
   * is transparently convered to a string.
   */
  static final class WrapPartition implements Partition<String> {
    private final Partition<Pair<LongWritable, Text>> wrap;

    WrapPartition(Partition<Pair<LongWritable, Text>> wrap) {
      this.wrap = Objects.requireNonNull(wrap);
    }

    @Override
    public Set<String> getLocations() {
      return wrap.getLocations();
    }

    @Override
    public Reader<String> openReader() throws IOException {
      return new WrapReader(this.wrap.openReader());
    }
  }

  /**
   * A standard URI based factory for instances of {@link SimpleHadoopTextFileSource}.
   */
  public static final class Factory implements DataSourceFactory {
    @Override
    @SuppressWarnings("unchecked")
    public <T> DataSource<T> get(URI uri, Settings settings) {
      return (DataSource<T>) new SimpleHadoopTextFileSource(
          uri.toString(), HadoopUtils.createConfiguration(settings));
    }
  }

  private final HadoopTextFileSource wrap;

  @Override
  public List<Partition<String>> getPartitions() {
    return this.wrap.getPartitions().stream().map(WrapPartition::new).collect(toList());
  }

  @Override
  public boolean isBounded() {
    return this.wrap.isBounded();
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
