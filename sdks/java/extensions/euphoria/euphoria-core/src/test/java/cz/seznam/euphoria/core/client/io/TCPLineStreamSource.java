

package cz.seznam.euphoria.core.client.io;

import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.util.Settings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A {@code DataSource} that reads input from a TCP socket with one partition.
 * Each input is a single line.
 */
public class TCPLineStreamSource implements DataSource<String> {

  private static class TCPLineIterator implements Iterator<String> {

    private final InputStream input;
    private final BufferedReader reader;

    public TCPLineIterator(String host, int port) throws IOException {
      Socket s = new Socket(host, port);
      input = s.getInputStream();
      reader = new BufferedReader(new InputStreamReader(input));
    }

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public String next() {
      try {
        return reader.readLine();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

  }

  final String host;
  final int port;

  public TCPLineStreamSource(String host, int port) {
    this.host = host;
    this.port = port;
  }




  @Override
  public List<Partition<String>> getPartitions() {
    return Arrays.asList(new Partition<String>() {

      @Override
      public Set<String> getLocations() {
        return Sets.newHashSet(host);
      }

      @Override
      public Iterator<String> iterator() {
        try {
          return new TCPLineIterator(host, port);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    });
  }

  @Override
  public boolean isBounded() {
    return false;
  }

  public static class Factory implements DataSourceFactory {
    @Override
    @SuppressWarnings("unchecked")
    public <T> DataSource<T> get(URI uri, Settings settings)
    {
      return (DataSource) new TCPLineStreamSource(uri.getHost(), uri.getPort());
    }
  }
}
