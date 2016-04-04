
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.util.Settings;
import java.net.URI;

/**
 * {@code IORegistry} that creates {@code DataSource} based on scheme.
 */
public class SchemeBasedIORegistry extends IORegistry {

  public static final String SCHEME_SOURCE_PREFIX = "euphoria.io.datasource.factory.";

  @Override
  public <T> DataSource<T> openSource(URI uri, Settings settings) throws Exception {
    String name = SCHEME_SOURCE_PREFIX + uri.getScheme();
    DataSourceFactory factory = getInstance(settings, name, DataSourceFactory.class);
    return factory.get(uri, settings);
  }

}
