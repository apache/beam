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
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.util.Settings;

import java.net.URI;

/**
 * {@code IORegistry} that creates {@code DataSource} based on scheme.
 */
public class SchemeBasedIORegistry extends IORegistry {

  public static final String SCHEME_SOURCE_PREFIX = "euphoria.io.datasource.factory.";

  public static final String SCHEME_SINK_PREFIX = "euphoria.io.datasink.factory.";

  @Override
  public <T> DataSource<T> openSource(URI uri, Settings settings) throws Exception {
    String name = SCHEME_SOURCE_PREFIX + uri.getScheme();
    DataSourceFactory factory = getInstance(settings, name, DataSourceFactory.class);
    return factory.get(uri, settings);
  }

  @Override
  public <T> DataSink<T> openSink(URI uri, Settings settings) throws Exception {
    String name = SCHEME_SINK_PREFIX + uri.getScheme();
    DataSinkFactory factory = getInstance(settings, name, DataSinkFactory.class);
    return factory.get(uri, settings);
  }
}
