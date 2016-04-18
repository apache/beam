package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.core.util.URIParams;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

/**
 * A sink to write to a specified print stream (typically
 * {@link java.lang.System#out}) using the produce element's
 * {@link Object#toString()} implementation.
 */
public class StdoutSink<T> extends DataSink<T> {

  public static class Factory implements DataSinkFactory {
    @Override
    public <T> DataSink<T> get(URI uri, Settings settings) {
      String cfg = URIParams.of(uri).getStringParam("cfg", null);
      boolean dumpPartitionId = settings.getBoolean(
          (cfg != null && !cfg.isEmpty() ? (cfg + ".") : "") + "dump-partition-id",
          false);
      return new StdoutSink<>(dumpPartitionId);
    }
  }

  static abstract class AbstractWriter<T> extends Writer<T> {
    final PrintStream out;

    AbstractWriter(PrintStream out) {
      this.out = out;
    }

    @Override
    public void commit() throws IOException {
      out.flush();
    }
  }

  static final class PlainWriter<T> extends AbstractWriter<T> {
    PlainWriter(PrintStream out) {
      super(out);
    }

    @Override
    public void write(T elem) throws IOException {
      out.println(elem);
    }
  }

  static final class PartitionIdWriter<T> extends AbstractWriter<T> {
    final int partitionId;
    final StringBuilder buf = new StringBuilder();

    PartitionIdWriter(PrintStream out, int partitionId) {
      super(out);
      this.partitionId = partitionId;
    }

    @Override
    public void write(T elem) throws IOException {
      // ~ make sure to issue only _one_ `out.println()` call to
      // avoid messing up the output with concurrent threads trying
      // to do the same
      buf.setLength(0);
      buf.append('[').append(partitionId).append("]: ").append(elem);
      out.println(buf);
    }
  }

  private final boolean dumpPartitionId;

  StdoutSink(boolean dumpPartitionId) {
    this.dumpPartitionId = dumpPartitionId;
  }

  @Override
  public Writer<T> openWriter(int partitionId) {
    PrintStream out = System.out;
    return dumpPartitionId
        ? new PartitionIdWriter<>(out, partitionId)
        : new PlainWriter<>(out);
  }

  @Override
  public void commit() throws IOException {
  }

  @Override
  public void rollback() {
  }
}
