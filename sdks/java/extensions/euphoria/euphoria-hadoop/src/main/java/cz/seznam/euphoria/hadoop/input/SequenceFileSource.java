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
package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.DataSourceFactory;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.net.URI;

/**
 * A data source reading hadoop based inputs as a sequences of records persisted in
 * (binary) sequence files.
 */
public class SequenceFileSource<K extends Writable, V extends Writable>
    extends HadoopSource<K, V> {

  /**
   * A standard URI based factory for instances of {@link SequenceFileSource}.
   */
  public static final class Factory implements DataSourceFactory {
    @Override
    @SuppressWarnings("unchecked")
    public <T> DataSource<T> get(URI uri, Settings settings) {
      return (DataSource<T>) new SequenceFileSource<>(
          uri.toString(), HadoopUtils.createConfiguration(settings));
    }
  }


  /**
   * Convenience constructor invoking
   * {@link #SequenceFileSource(String, Configuration)} with a newly created
   * hadoop configuration.
   *
   * @throws NullPointerException if the given path is {@code null}
   */
  public SequenceFileSource(String path) {
    this(path, new Configuration());
  }

  /**
   * Constructs a data source based on hadoop's {@link SequenceFileInputFormat}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  public SequenceFileSource(String path, Configuration hadoopConfig) {
    super((Class) Writable.class, (Class) Writable.class,
        (Class) SequenceFileInputFormat.class, hadoopConfig);
    hadoopConfig.set(FileInputFormat.INPUT_DIR, path);
  }

}
