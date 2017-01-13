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
 * A convenience data sink based on {@link HadoopTextFileDataSink} to provide
 * a simpler API consuming only a single value for emission (as opposed to the
 * more general key/value pair.)
 *
 * @param <V> the type of value emitted (as text)
 */
public class SimpleHadoopTextFileDataSink<V> implements DataSink<V> {

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
   * A standard URI based factory for instance of {@link SimpleHadoopTextFileDataSink}.
   */
  public static final class Factory implements DataSinkFactory {
    @Override
    @SuppressWarnings("unchecked")
    public <T> DataSink<T> get(URI uri, Settings settings) {
      return (DataSink<T>) new SimpleHadoopTextFileDataSink<>(
          uri.toString(), HadoopUtils.createConfiguration(settings));
    }
  }

  private final HadoopTextFileDataSink<Void, V> wrap;

  /**
   * Convenience constructor invoking
   * {@link #SimpleHadoopTextFileDataSink(String, Configuration)} with a newly created
   * hadoop configuration.
   *
   * @param path the path where to place the output to
   *
   * @throws NullPointerException if any of the given parameters is {@code null}
   */
  public SimpleHadoopTextFileDataSink(String path) {
    this(path, new Configuration());
  }

  /**
   * Constructs a data sink based on {@link HadoopTextFileDataSink}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  public SimpleHadoopTextFileDataSink(String path, Configuration hadoopConfig) {
    this.wrap = new HadoopTextFileDataSink<>(path, hadoopConfig);
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
