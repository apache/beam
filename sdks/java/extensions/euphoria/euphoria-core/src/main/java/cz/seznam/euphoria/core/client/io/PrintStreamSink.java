package cz.seznam.euphoria.core.client.io;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Objects;

/**
 * A sink to write to a specified print stream (typically
 * {@link java.lang.System#out}) using the produce element's
 * {@link Object#toString()} implementation.
 */
public class PrintStreamSink<T> extends DataSink<T> {

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

  private final PrintStream out;
  private final boolean dumpPartitionId;

  public PrintStreamSink(PrintStream out) {
    this(out, false);
  }

  public PrintStreamSink(PrintStream out, boolean dumpPartitionId) {
    this.out = Objects.requireNonNull(out);
    this.dumpPartitionId = dumpPartitionId;
  }

  @Override
  public Writer<T> openWriter(int partitionId) {
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
