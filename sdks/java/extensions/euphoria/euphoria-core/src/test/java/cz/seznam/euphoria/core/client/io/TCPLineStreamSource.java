

package cz.seznam.euphoria.core.client.io;

import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.util.Settings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * A {@code DataSource} that reads input from a TCP socket with one partition.
 * Each input is a single line.
 */
public class TCPLineStreamSource implements DataSource<String> {

  private static class TCPLineReader implements Reader<String> {

    private final BufferedReader reader;

    public TCPLineReader(String host, int port) throws IOException {
      Socket s = new Socket(host, port);
      reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
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

    @Override
    public void close() throws IOException {
      reader.close();
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
      public Reader<String> openReader() throws IOException {
        return new TCPLineReader(host, port);
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
