package cz.seznam.euphoria.core.client.util;

import cz.seznam.euphoria.core.util.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IOUtilsTest {

  @Test(expected = IOException.class)
  public void testOneIOException() throws IOException {
    IOUtils.forEach(Arrays.asList(1, 2, 3), (i) -> {
      if (i == 2) {
        throw new IOException("Number: " + i);
      }
    });
  }

  @Test
  public void testSuppressedIOException() throws IOException {
    try {
      IOUtils.forEach(Arrays.asList(1, 2, 3), (i) -> {
        throw new IOException("Number: " + i);
      });
    } catch (Exception e) {
      assertEquals(2, e.getSuppressed().length); //two supressed exceptions and one thrown
      assertTrue(e instanceof IOException);
      assertEquals("Number: 1", e.getMessage());
    }
  }

  @Test(expected = IOException.class)
  public void testStreamIOException() throws IOException {

    IOUtils.forEach(Stream.of(1, 2, 3), (i) -> {
      if (i == 2) {
        throw new IOException("Number: " + i);
      }
    });
  }

}
