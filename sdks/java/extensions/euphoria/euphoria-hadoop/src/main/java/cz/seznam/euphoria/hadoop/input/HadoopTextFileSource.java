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
import cz.seznam.euphoria.core.client.io.DataSourceFactory;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.net.URI;
import java.util.Objects;

/**
 * A data source reading hadoop based inputs as a sequences of text lines.
 */
public class HadoopTextFileSource extends HadoopSource<LongWritable, Text> {

  /**
   * A standard URI based factory for instances of {@link HadoopTextFileSource}.
   */
  public static final class Factory implements DataSourceFactory {
    @Override
    @SuppressWarnings("unchecked")
    public <T> DataSource<T> get(URI uri, Settings settings) {
      return (DataSource<T>) new HadoopTextFileSource(
          uri.toString(), HadoopUtils.createConfiguration(settings));
    }
  }


  /**
   * Convenience constructor invoking
   * {@link #HadoopTextFileSource(String, Configuration)} with a newly created
   * hadoop configuration.
   *
   * @param path the path to read data from
   *
   * @throws NullPointerException if the given path is {@code null}
   */
  public HadoopTextFileSource(String path) {
    this(path, new Configuration());
  }

  /**
   * Constructs a data source based on hadoop's {@link TextInputFormat}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  public HadoopTextFileSource(String path, Configuration hadoopConfig) {
    super(LongWritable.class, Text.class, TextInputFormat.class, hadoopConfig);
    hadoopConfig.set(TextInputFormat.INPUT_DIR, Objects.requireNonNull(path));
  }
}
