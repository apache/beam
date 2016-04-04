
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.util.Settings;
import java.io.Serializable;
import java.net.URI;

/**
 * A factory for {@code DataSource}s.
 */
public interface DataSourceFactory extends Serializable {

  <T> DataSource<T> get(URI uri, Settings settings);

}
