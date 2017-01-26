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
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSinkFactory;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

/**
 * A convenience data sink based on {@link HadoopTextFileSink} to provide
 * a simpler API consuming only a single value for emission (as opposed to the
 * more general key/value pair.)
 *
 * @param <V> the type of value emitted (as text)
 */
public class SimpleHadoopTextFileSink<V> implements DataSink<V> {

  /**
   * Wraps a {@code Writer<Pair<Void, V>>} and provides an API as {@code Writer<W>}.
   */
  static final class WrapWriter<V> implements Writer<V> {
    private final Writer<Pair<Void, V>> wrap;

    WrapWriter(Writer<Pair<Void, V>> wrap) {
      this.wrap = Objects.requireNonNull(wrap);
    }

    @Override
    public void write(V elem) throws IOException {
      wrap.write(Pair.of(null, elem));
    }

    @Override
    public void commit() throws IOException {
      wrap.commit();
    }

    @Override
    public void close() throws IOException {
      wrap.commit();
    }
  }

  /**
   * A standard URI based factory for instance of {@link SimpleHadoopTextFileSink}.
   */
  public static final class Factory implements DataSinkFactory {
    @Override
    @SuppressWarnings("unchecked")
    public <T> DataSink<T> get(URI uri, Settings settings) {
      return (DataSink<T>) new SimpleHadoopTextFileSink<>(
          uri.toString(), HadoopUtils.createConfiguration(settings));
    }
  }

  private final HadoopTextFileSink<Void, V> wrap;

  /**
   * Convenience constructor invoking
   * {@link #SimpleHadoopTextFileSink(String, Configuration)} with a newly created
   * hadoop configuration.
   *
   * @param path the path where to place the output to
   *
   * @throws NullPointerException if any of the given parameters is {@code null}
   */
  public SimpleHadoopTextFileSink(String path) {
    this(path, new Configuration());
  }

  /**
   * Constructs a data sink based on {@link HadoopTextFileSink}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  public SimpleHadoopTextFileSink(String path, Configuration hadoopConfig) {
    this.wrap = new HadoopTextFileSink<>(path, hadoopConfig);
  }

  @Override
  public Writer<V> openWriter(int partitionId) {
    return new WrapWriter<>(wrap.openWriter(partitionId));
  }

  @Override
  public void commit() throws IOException {
    wrap.commit();
  }

  @Override
  public void rollback() throws IOException {
    wrap.rollback();
  }
}
