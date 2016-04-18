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

  public <T> DataSink<T> openSink(URI uri, Settings settings) throws Exception {
    String name = SCHEME_SINK_PREFIX + uri.getScheme();
    DataSinkFactory factory = getInstance(settings, name, DataSinkFactory.class);
    return factory.get(uri, settings);
  }
}
