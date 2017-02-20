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
 * Factory for {@link DataSource}s and {@link DataSink}s based on URIs.
 * <p>
 * The registry instantiates {@link SchemeBasedIORegistry} by default to resolve
 * a URI to a data source or sink. If {@link #REGISTRY_IMPL_CONF} is defined
 * in the supplied configuration bundle, it is expected to define a subclass of
 * {@link IORegistry} and have a public, default constructor. A new instance of
 * this sub-class will be created for every URI resolve request and is then responsible
 * for creating the corresponding data source or sink.
 */
public abstract class IORegistry {

  /**
   * The configuration key specifying a sub-class of {@link IORegistry} to
   * instantiate to handle URI to data source/sink translation requests.
   */
  public static final String REGISTRY_IMPL_CONF = "euphoria.io.registry.impl";


  /**
   * Retrieves an {@link IORegistry} from the specified the configuration.
   * Falls back to {@link SchemeBasedIORegistry} if none is explicitly defined.
   *
   * @param settings the configuration settings
   *
   * @return a {@link IORegistry}
   *
   * @throws Exception if instantiating the configured {@link IORegistry} fails
   *          for some reason or if the configured registry is not sub-class
   *          of {@link IORegistry}
   */
  public static IORegistry get(Settings settings) throws Exception {
    return getInstance(settings, REGISTRY_IMPL_CONF,
        IORegistry.class, new SchemeBasedIORegistry());
  }


  /**
   * Retrieve {@code DataSource} for URI.
   *
   * @param <T> the type of elements provided by the resulting data source
   *
   * @param uri the URI specifying a data source
   * @param settings the settings to utilize while locating the data source
   *
   * @return a data source identified by the specified uri
   *
   * @throws Exception if it is not possible to identify the source from the given uri
   */
  public abstract <T> DataSource<T> openSource(URI uri, Settings settings) throws Exception;

  public abstract <T> DataSink<T> openSink(URI uri, Settings settings) throws Exception;

  /*
   * Create new instance from config value.
   */
  static <T> T getInstance(Settings settings, String name, Class<T> clz)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {

    T instance = getInstance(settings, name, clz, null);
    if (instance == null) {
      throw new IllegalStateException("No config option " + name + " found");
    }
    return instance;
  }


  /*
   * Create new instance from config value.
   */
  @SuppressWarnings("unchecked")
  private static <T> T getInstance(Settings settings, String name, Class<T> clz, T def)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    
    String clzName = settings.getString(name, null);
    if (clzName == null) {
      return def;
    }
    return (T) Thread.currentThread().getContextClassLoader()
        .loadClass(clzName).newInstance();
  }

}
