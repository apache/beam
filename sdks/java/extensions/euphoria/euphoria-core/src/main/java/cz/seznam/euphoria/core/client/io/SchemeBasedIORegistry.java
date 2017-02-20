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
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.util.Settings;

import java.net.URI;

/**
 * An {@code IORegistry} creating {@code DataSource} based on a URI's schema.
 *
 * Given a configuration bundle and a URI, this registry creates an associated
 * data source as follows:
 * <ol>
 *   <li>Extract the schema from the given URI</li>
 *   <li>Lookup the value under the key "{@link #SCHEME_SOURCE_PREFIX} + schema" where
 *       schema represents the schema value extracted from the given URI</li>
 *   <li>If no such value is defined, just fail.</li>
 *   <li>Otherwise validate that the value names an existing class which implements
 *        {@link DataSourceFactory}.</li>
 *   <li>Instantiate the class using its default public constructor.</li>
 *   <li>Invoke {@link DataSourceFactory#get(URI, Settings)} on the new factory instance
 *        passing on the original URI and configuration values.</li>
 *   <li>Return the result of the factory's {@code get} method invocation.</li>
 * </ol>
 *
 * Similarly, the same applies to constructing sinks. The corresponding key prefix is
 * {@link #SCHEME_SINK_PREFIX} and the factory interface {@link DataSinkFactory}.
 */
public class SchemeBasedIORegistry extends IORegistry {

  /** Key prefix specifying associations of schemes to particular data source factories. */
  public static final String SCHEME_SOURCE_PREFIX = "euphoria.io.datasource.factory.";

  /** Key prefix specifying associations of schems to particular data sink factories. */
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
