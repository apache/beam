
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.util.Settings;
import java.net.URI;

/**
 * Factory of {@code DataSource} from URI and settings.
 */
public abstract class IORegistry {


  private static final String REGISTRY_IMPL_CONF = "euphoria.io.registry.impl";
  

  public static IORegistry get(Settings settings) throws Exception {
    return getInstance(settings, REGISTRY_IMPL_CONF,
        IORegistry.class, new SchemeBasedIORegistry());
  }


  /**
   * Retrieve {@code DataSource} for URI.
   */
  public abstract <T> DataSource<T> openSource(URI uri, Settings settings) throws Exception;


  /**
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


  /**
   * Create new instance from config value.
   */
  @SuppressWarnings("unchecked")
  static <T> T getInstance(Settings settings, String name, Class<T> clz, T def)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    
    String clzName = settings.getString(name, null);
    if (clzName == null) {
      return def;
    }
    return (T) Thread.currentThread().getContextClassLoader()
        .loadClass(clzName).newInstance();
  }

}
