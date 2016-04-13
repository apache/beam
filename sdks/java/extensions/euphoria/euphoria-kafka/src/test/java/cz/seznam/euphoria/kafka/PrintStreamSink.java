package cz.seznam.euphoria.kafka;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Objects;

public class PrintStreamSink<T> extends DataSink<T> {

  static abstract class AbstractWriter<T> extends Writer<T> {
    final PrintStream out;

    AbstractWriter(PrintStream out) {
      this.out = Objects.requireNonNull(out);
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
    this.out = out;
    this.dumpPartitionId = dumpPartitionId;
  }

  @Override
  public Writer<T> openWriter(int partId) {
    return dumpPartitionId
        ? new PartitionIdWriter<>(out, partId)
        : new PlainWriter<>(out);
  }

  @Override
  public void commit() throws IOException {
  }

  @Override
  public void rollback() {
  }
}
