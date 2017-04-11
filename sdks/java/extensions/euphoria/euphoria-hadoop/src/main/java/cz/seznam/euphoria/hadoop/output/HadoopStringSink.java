/**
 * Copyright 2016 Seznam.cz, a.s.
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
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSinkFactory;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

/**
 * A convenience data sink based on hadoop's {@link TextOutputFormat} for
 * directly accepting String values.
 */
public class HadoopStringSink implements DataSink<String> {

  /**
   * A standard URI based factory for instance of {@link HadoopStringSink}.
   */
  public static final class Factory implements DataSinkFactory {
    @Override
    @SuppressWarnings("unchecked")
    public <T> DataSink<T> get(URI uri, Settings settings) {
      return (DataSink<T>) new HadoopStringSink(
          uri.toString(), HadoopUtils.createConfiguration(settings));
    }
  }

  private final HadoopTextFileSink<String, NullWritable> impl;

  /**
   * Convenience constructor invoking {@link #HadoopStringSink(String, Configuration)}
   * with a newly created hadoop configuration.
   *
   * @param path the path where to place the output to
   *
   * @throws NullPointerException if any of the given parameters is {@code null}
   */
  public HadoopStringSink(String path) {
    this(path, new Configuration());
  }

  /**
   * Constructs a data sink based on hadoop's {@link TextOutputFormat}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  public HadoopStringSink(String path, Configuration hadoopConfig) {
    impl = new HadoopTextFileSink<>(path, hadoopConfig);
  }

  @Override
  public void initialize() {
    impl.initialize();
  }

  @Override
  public Writer<String> openWriter(int partitionId) {
    return new WriterAdapter<>(impl.openWriter(partitionId));
  }

  @Override
  public void commit() throws IOException {
    impl.commit();
  }

  @Override
  public void rollback() throws IOException {
    impl.rollback();
  }

  private static final class WriterAdapter<E> implements Writer<E> {
    private final Writer<Pair<E, NullWritable>> impl;

    WriterAdapter(Writer<Pair<E, NullWritable>> impl) {
      this.impl = Objects.requireNonNull(impl);
    }

    @Override
    public void write(E elem) throws IOException {
      impl.write(Pair.of(elem, NullWritable.get()));
    }

    @Override
    public void commit() throws IOException {
      impl.commit();
    }

    @Override
    public void close() throws IOException {
      impl.close();
    }
  }
}
