package cz.seznam.euphoria.core.client.io;

import com.google.common.collect.Iterables;
import org.junit.Test;

import static org.junit.Assert.*;

public class ListDataSinkTest {

  @Test
  public void testMultipleSinks() throws Exception {
    ListDataSink<String> sink1 = ListDataSink.get(1);
    ListDataSink<String> sink2 = ListDataSink.get(2);

    // write to first sink
    Writer<String> w = sink1.openWriter(0);
    w.write("first");
    w.commit();

    // write to seconds sink
    w = sink2.openWriter(0);
    w.write("second-0");
    w.commit();

    w = sink2.openWriter(1);
    w.write("second-1");
    w.commit();

    assertEquals("first", Iterables.getOnlyElement(sink1.getOutput(0)));

    assertEquals("second-0", Iterables.getOnlyElement(sink2.getOutput(0)));
    assertEquals("second-1", Iterables.getOnlyElement(sink2.getOutput(1)));
  }
}