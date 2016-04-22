package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.util.Settings;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InMemFileSystemTest {

  private InMemFileSystem fs = InMemFileSystem.get();

  @Before
  public void setUp() {
    fs.reset();
  }

  @Test
  public void testReadSingleBounded() throws IOException {
    fs.setFile("/tmp/01.txt", asList("one", "two", "three"));
    fs.setFile("/tmp/02.txt", asList("four", "five", "six", "seven"));

    DataSource<String> ds = openDatasource("/tmp/01.txt");
    List<Partition<String>> ps = ds.getPartitions();
    assertEquals(1, ps.size());
    assertEquals(asList("one", "two", "three"), readAll(ps.get(0)));
  }

  @Test
  public void testReadMultiBounded() throws IOException {
    fs.setFile("/tmp/01.txt", asList("one", "two", "three"));
    fs.setFile("/tmp/02.txt", asList("four", "five", "six", "seven"));

    DataSource<String> ds = openDatasource("/tmp/");
    List<Partition<String>> ps = ds.getPartitions();
    assertEquals(2, ps.size());
    assertEquals(asList("one", "two", "three"), readAll(ps.get(0)));
    assertEquals(asList("four", "five", "six", "seven"), readAll(ps.get(1)));
  }

  @Test
  public void testReadSingleUnbounded() throws IOException {
    Duration duration = Duration.ofMillis(100);
    fs.setFile("/tmp/01.txt", duration, asList("a", "b", "c", "d", "e"));

    DataSource<String> ds = openDatasource("/tmp/01.txt");
    assertEquals(false, ds.isBounded());

    List<Partition<String>> ps = ds.getPartitions();
    assertEquals(1, ps.size());
    long start = System.nanoTime();
    List<String> expected = asList("a", "b", "c", "d", "e");
    assertEquals(expected, readAll(ps.get(0)));
    long end = System.nanoTime();
    assertTrue(TimeUnit.NANOSECONDS.toMillis(end-start) >= expected.size()*duration.toMillis());
  }

  @SuppressWarnings("unchecked")
  private <T> DataSource<T> openDatasource(String path) {
    InMemFileSystem.Factory fact = new InMemFileSystem.Factory();
    return (DataSource) fact.get(URI.create(path), new Settings());
  }

  private <T> List<T> readAll(Partition<T> p) throws IOException {
    List<T> xs = new ArrayList<>();
    try (Reader<T> r = p.openReader()) {
      while (r.hasNext()) {
        xs.add(r.next());
      }
    }
    return xs;
  }

}