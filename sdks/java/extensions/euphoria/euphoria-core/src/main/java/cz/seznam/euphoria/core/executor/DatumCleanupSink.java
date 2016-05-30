package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;

import java.io.IOException;
import java.util.Objects;

/**
 * A sink to filter out inmem specific messages; An instance of this sink wraps
 * typically a user provided output sink.
 */
class DatumCleanupSink implements DataSink<Object> {

  static final class UnwrapDatumWriter implements Writer {
    private final Writer wrap;

    UnwrapDatumWriter(Writer wrap) {
      this.wrap = wrap;
    }

    @Override
    public void write(Object elem) throws IOException {
      if (elem instanceof InMemExecutor.EndOfWindow) {
        // ~ ignore
        return;
      }
      if (elem instanceof Datum) {
        // ~ strip the datum information from the emitted element
        this.wrap.write(((Datum) elem).element);
        return;
      }
      // ~ for debugging purposes
      throw new IllegalStateException("Unknown element: " + elem);
    }

    @Override
    public void commit() throws IOException {
      this.wrap.commit();
    }

    @Override
    public void close() throws IOException {
      this.wrap.close();
    }
  }

  private final DataSink wrap;

  DatumCleanupSink(DataSink wrap) {
    this.wrap = Objects.requireNonNull(wrap);
  }

  @Override
  public Writer openWriter(int partitionId) {
    return new UnwrapDatumWriter(this.wrap.openWriter(partitionId));
  }

  @Override
  public void commit() throws IOException {
    this.wrap.commit();
  }

  @Override
  public void rollback() {
    this.wrap.rollback();
  }
}
